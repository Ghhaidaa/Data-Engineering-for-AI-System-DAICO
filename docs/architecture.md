# Architecture

How the five stages fit together, what each component guarantees, and why the design choices were made.

## Data flow

| # | Stage | Input | Output | Notebook Section |
|---|---|---|---|---|
| 1 | Ingestion | Synthetic publisher submissions | Kafka topic `books` → `valid_books.csv` + `rejected_books.csv` | Stage 1-2 |
| 2 | Bronze | `valid_books.csv` | `/content/delta/bronze` | Stage 1-2 |
| 3 | Silver | Bronze | `/content/delta/silver` | Stage 1-2 |
| 4 | Gold | Silver | `/content/delta/gold` | Stage 1-2 |
| 5 | RAG | Gold-derived documents | Cited answers | Stage 4 |
| 6 | Quality Gate | Bronze/Silver/Gold | `quality_gate_result.json` | Stage 5 |
| 7 | Orchestration | All stages | `sdaia_books_pipeline` DAG run | Stage 6 |

Orchestration: `/root/airflow/dags/sdaia_books_dag.py`. Lineage: `emit_lineage_event()`, called from the Ingestion, Lakehouse, Quality Gate, and RAG stages, and from inside the Airflow `quality_gate` task.

## 1. Ingestion — schema validation

100 synthetic book records are generated (title, author, publisher, category, language, publication year, submission date), with 5 records deliberately corrupted (missing ISBN, empty title, unsupported language, future publication year, malformed date) to prove the failure path.

Each record is validated against a **Pydantic data contract** (`BookRecord`), which enforces:

| Rule | Rejects |
|---|---|
| `isbn` non-empty | missing ISBN |
| `title` non-empty (stripped) | blank titles |
| `language` in `{Arabic, English}` | unsupported languages |
| `publication_year` not in the future | invalid future years |
| `submission_date` matches `%Y-%m-%d` | malformed dates |

**Routing:** valid records are written to `valid_books.csv` and published to the Kafka topic `books` via a real `KafkaProducer`; rejected records are written to `rejected_books.csv` with the Pydantic error message attached as the rejection reason. A real `KafkaConsumer` reads the `books` topic back to confirm delivery.

## 2. Lakehouse

**Bronze — append-only.** `valid_books.csv` is read into Spark, stamped with `ingestion_time`, and written as a Delta table (`/content/delta/bronze`).

**Silver — cleaned.** Bronze is deduplicated and re-typed (`publication_year` as `int`, `submission_date` as `date`), then written to `/content/delta/silver`.

**MERGE (upsert).** An update batch keyed on `book_id` is merged into Silver:

```python
silver_table.alias("target").merge(
    updates_df.alias("source"),
    "target.book_id = source.book_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

Matched `book_id`s are updated in place; unmatched ones are inserted — demonstrated live against `book_id = 46`.

**Schema enforcement.** Appending a row with an undeclared `extra_column` is attempted against Silver; Delta rejects the write (`DELTA_FAILED_TO_MERGE_FIELDS`), proving the table's schema is enforced, not just declared.

**Gold — a real aggregate.** Silver is grouped by `category` and `language`, producing `number_of_books` per group — a rollup, not a filtered copy of Silver.

## 3. RAG

| Step | Implementation |
|---|---|
| Document source | Gold-layer rows converted into descriptive text per category/language |
| Chunking | Word-based, 15 words per chunk, 5-word overlap |
| Embeddings | `all-MiniLM-L6-v2` (sentence-transformers) |
| Vector store | ChromaDB |
| Keyword search | `rank_bm25.BM25Okapi` |
| Fusion | Reciprocal Rank Fusion (k = 60) |
| Reranking | `cross-encoder/ms-marco-MiniLM-L-6-v2` |
| Citation | Answer is grounded in the top reranked chunk, with source category/language/doc_id attached |

Dense retrieval finds semantically similar passages; BM25 finds exact term matches. RRF merges both ranked lists without manual weighting, and the cross-encoder rescoring picks the most relevant passage before the answer is generated.

## 4. Quality Gate

A **Great Expectations 1.x** validator (fluent API — ephemeral context, pandas data source, dataframe asset, batch definition) checks:

- Bronze: `book_id`, `isbn` not null
- Silver: `book_id` not null, `language` in `{Arabic, English}`, `publication_year` between 1900–2026
- Gold: `number_of_books` ≥ 1

All results are combined into a single `all_passed` decision, then persisted to `/content/quality_gate_result.json` so the Airflow task can read it independently of the notebook's in-memory state. The failure path is proven separately: a corrupted copy of Silver (invalid `language` value) is validated and confirmed to FAIL.

## 5. Orchestration and lineage
start → data_ingestion → delta_lakehouse → quality_gate → rag_pipeline → end
The DAG (`sdaia_books_pipeline`) is written to `/root/airflow/dags/sdaia_books_dag.py` so Airflow can serialize and execute it. The `quality_gate` task reads `quality_gate_result.json` and raises an exception if `all_passed` is `False` — with Airflow's default `all_success` trigger rule, `rag_pipeline` and `end` are then never executed. This is proven by running `airflow dags test` twice: once against a passing result, once against a forced failure.

Each stage — Ingestion, Lakehouse, Quality Gate, RAG — emits real `openlineage-python` `RunEvent`s (START / COMPLETE / FAIL) via `OpenLineageClient` with a `ConsoleTransport`, under the namespace `sdaia-books-platform`.

## Configuration / Environment

- Kafka broker: `localhost:9092` (KRaft mode)
- Kafka topic: `books`
- Delta paths: `/content/delta/bronze`, `/content/delta/silver`, `/content/delta/gold`
- Quality Gate result file: `/content/quality_gate_result.json`
- Airflow DAG file: `/root/airflow/dags/sdaia_books_dag.py`
- Airflow DAG ID: `sdaia_books_pipeline`

## Design notes

**Why MERGE on `book_id`?** It is the natural business key for a book registration record, and keeps the Delta MERGE condition a single-column match.

**Why Gold overwrites instead of merging?** It is a full recomputation from Silver, the source of truth — cheap to rebuild and immune to drift between layers.
