# Data Engineering for AI System-Capstone

Students: Ghaida Bin maadi, Shaden Alghannam, Reema AlDayel

Program: SDAIA Academy — Modern Data Engineering for AI Systems

Session dates: 19 July 2026 – 23 July 2026

Trainer: Mohammed Albeladi

## Project Overview

An end-to-end data engineering pipeline for the SDAIA Books Platform — validating,
streaming, and storing book registration data submitted by publishers across
Saudi Arabia, from raw submissions all the way to validated analytics tables
and a grounded RAG question-answering layer.

The source is a set of synthetic publisher submissions (100 records) generated
to mimic real book registration data, with 5 records deliberately corrupted
(missing ISBN, empty title, unsupported language, future publication year,
malformed date) to prove that bad data is caught rather than silently absorbed.

This project puts a machine-enforceable contract at the ingestion boundary and
a quality gate in front of the analytics layer, so invalid records are stopped,
recorded with a reason, and never silently pushed downstream.

**Scope:** streaming ingestion with schema validation, a three-layer Delta
lakehouse with an incremental upsert, a hybrid-search RAG pipeline, Airflow
orchestration, and per-stage quality gating and lineage.

## Pipeline Architecture
## Pipeline Architecture

    Kafka producer
          |
          v
    Kafka consumer (topic: books)
          |
          v
    Pydantic contract validation --> rejected_books.csv (rejection reason attached)
          |
          | (valid records only)
          v
    Bronze (Delta, append-only)
          |
          v
    Silver (Delta MERGE upsert on book_id)
          |
          +----------------> Gold (Delta aggregate: books per category/language)
          |
          v
    Quality Gate (Great Expectations) --> raises on failure, halting the pipeline
          |
          +----------------> RAG pipeline

Every stage emits OpenLineage START / COMPLETE / FAIL events.

## 1. Data Ingestion

A real Kafka ingestion path built on `kafka-python`.

- **Producer** streams valid book records into the `books` topic as JSON.
- **Validation** happens against the `BookRecord` Pydantic contract: non-empty
  ISBN, non-empty title, `language` in `{Arabic, English}`, publication year
  not in the future, and a well-formed `submission_date`.
- **Accepted records** are written to `valid_books.csv` and published to Kafka.
- **Rejected records** are written to `rejected_books.csv`, carrying the exact
  Pydantic error message as the rejection reason.
- **Consumer** reads the `books` topic back with a real `KafkaConsumer` to
  confirm delivery.

## 2. Delta Lakehouse

Bronze / Silver / Gold on `pyspark` + `delta-spark`.

**Bronze — append-only.** Valid records land exactly as they arrived, stamped
with `ingestion_time`.

**Silver — a real Delta MERGE** keyed on `book_id`. Matched keys are updated
in place, unmatched keys are inserted, in one atomic transaction — demonstrated
live against `book_id = 46`. Schema enforcement is proven explicitly: a write
carrying an undeclared `extra_column` is refused by Delta rather than silently
widening the table.

**Gold — a genuine aggregate**, not a filtered copy of Silver. Grouped by
`category` × `language`, producing `number_of_books` per group.

## 3. RAG Pipeline

| Step | Implementation |
|---|---|
| Chunking | Word-based, 15 words per chunk, 5-word overlap |
| Embeddings | `all-MiniLM-L6-v2` (Sentence Transformers) |
| Vector store | ChromaDB |
| Keyword search | `rank_bm25.BM25Okapi` |
| Fusion | Reciprocal Rank Fusion, k = 60, parameter-free |
| Reranking | `cross-encoder/ms-marco-MiniLM-L-6-v2` |
| Citation | Answer is grounded in the top reranked chunk, with source category/language/doc_id attached |

Dense retrieval finds semantically similar passages; BM25 finds exact term
matches. RRF merges both ranked lists without manual weighting, then the
cross-encoder rescoring picks the most relevant passage before the answer
is generated.

## 4. Pipeline Orchestration

An Apache Airflow DAG (`sdaia_books_pipeline`, 6 tasks) wires every stage together:
## Pipeline Orchestration

An Apache Airflow DAG (`sdaia_books_pipeline`, 6 tasks) wires every stage together:

    start
      |
      v
    data_ingestion
      |
      v
    delta_lakehouse
      |
      v
    quality_gate
      |
      v
    rag_pipeline
      |
      v
    end

`rag_pipeline` and `end` sit downstream of `quality_gate` with Airflow's
default `all_success` trigger rule, so a failed gate leaves both skipped —
the pipeline halts before anything downstream runs on unvalidated data.

## 5. Data Quality and Lineage

**Quality gate.** A Great Expectations 1.x validator on Bronze, Silver, and
Gold: non-null `book_id`/`isbn`, `language` in `{Arabic, English}`,
`publication_year` between 1900–2026, and `number_of_books` ≥ 1. The combined
result is persisted to `quality_gate_result.json` so the Airflow task can
read it independently. The gate is not advisory — it is load-bearing.

**Lineage.** The `quality_gate` task (and the Ingestion, Lakehouse, and RAG
stages) emit real `openlineage-python` `RunEvent`s: START on entry, COMPLETE
on success, FAIL if validation fails — then the exception is re-raised so
Airflow still fails the task. Events are emitted via `OpenLineageClient` with
a `ConsoleTransport`, under the namespace `sdaia-books-platform`.

## Executed Evidence

`notebooks/SDAIA_Books_Platform.ipynb` contains a full end-to-end run with all
output captured — open it directly on GitHub, no re-run needed. It includes:

- a local Kafka (KRaft) broker started and confirmed listening on `localhost:9092`
- producer and consumer output with real contract rejections
- Bronze append, Silver MERGE metrics
- the schema-enforcement rejection with Delta's own error message
- the Gold aggregate table
- the full RAG run: vector + BM25 candidates, RRF fusion, reranked results, a cited answer
- the Great Expectations checkpoint results and the OpenLineage event log
- the Airflow DAG written to file and executed via `airflow dags test`
- a forced quality-gate failure, showing the pipeline halt

Design rationale and component-level detail: `docs/architecture.md`

## Technologies Used

Python · Apache Kafka (`kafka-python`) · PySpark · Delta Lake (`delta-spark`) ·
Apache Airflow · Great Expectations · OpenLineage · ChromaDB ·
Sentence Transformers · BM25 (`rank-bm25`) · Pydantic v2

## How to Run

### Prerequisites

- Python 3.10+ (Google Colab environment used for development)
- JDK 17 — required by both Spark and the Kafka broker
- No external services required — Kafka and Airflow are started locally
  inside the notebook

### 1. Install dependencies
pip install -r requirements.txt
### 2. Run the pipeline

Open `notebooks/SDAIA_Books_Platform.ipynb` in Google Colab and run all cells
in order (Runtime → Run all). The notebook downloads and starts a local
Kafka (KRaft) broker, sets up Spark + Delta, and writes the Airflow DAG to
`/root/airflow/dags/sdaia_books_dag.py` before executing it.

## Expected Output

A successful run produces, in order:

| Stage | What you should see |
|---|---|
| Ingestion | 100 records generated; 95 valid (95%), 5 rejected (5%) with rejection reasons in `rejected_books.csv`; 95 records published to Kafka |
| Bronze | 95 records appended to the Delta Bronze table |
| Silver | 95 records after cleaning; a schema-enforcement rejection message from Delta |
| Gold | 14 aggregate rows (books per category/language) |
| RAG | 14 documents → 70 chunks; per query: vector + BM25 candidates, RRF fusion, cross-encoder reranking, a cited answer |
| Quality gate | 5 Great Expectations checks, all PASSED, `all_passed=True` |
| Lineage | A START and a matching COMPLETE event per stage under namespace `sdaia-books-platform` |
| Orchestration | `airflow dags test` succeeds end-to-end on valid data; on forced-failure data, `quality_gate` fails and `rag_pipeline`/`end` never run |

## Repository Structure
├── notebooks/
│ └── SDAIA_Books_Platform.ipynb # Full executed pipeline (all stages)
│
├── src/
│ ├── schemas/
│ │ └── book_record.py # Pydantic data contract
│ ├── lineage/
│ │ └── emitter.py # OpenLineage START / COMPLETE / FAIL
│ └── quality/
│ └── expectations.py # Great Expectations checks reference
│
├── docs/
│ └── architecture.md # Design rationale and component detail
│
├── requirements.txt
├── .gitignore
└── README.md
## Training Attribution

Completed as part of **Modern Data Engineering for AI Systems** — SDAIA Academy

Cohort / session dates: 19 July 2026 – 23 July 2026 Trainer: Mohammed Albeladi

SDAIA Academy on GitHub: https://github.com/SDAIAAcademy
