from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timezone
from pathlib import Path
import json
import uuid

from openlineage.client import OpenLineageClient
from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport
from openlineage.client.run import RunEvent, RunState, Run, Job

ol_client = OpenLineageClient(
    transport=ConsoleTransport(ConsoleConfig())
)

REQUIRED_COLUMNS = [
    "book_id",
    "isbn",
    "title",
    "author",
    "publisher_name",
    "publisher_city",
    "category",
    "language",
    "publication_year",
    "submission_date",
]


def emit_lineage_event(job_name, event_type, run_id=None):
    """Emit an OpenLineage event for an Airflow pipeline stage."""
    run_id = run_id or str(uuid.uuid4())

    event = RunEvent(
        eventType=getattr(RunState, event_type),
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id),
        job=Job(
            namespace="sdaia-books-platform",
            name=job_name,
        ),
        producer="sdaia-books-platform-pipeline",
    )

    ol_client.emit(event)
    return run_id


def data_ingestion_task():
    """
    Real ingestion task:
    1. Reads the validated source dataset.
    2. Checks the required schema.
    3. Sends every valid record to the Kafka books topic.
    """
    import pandas as pd
    from kafka import KafkaProducer

    run_id = emit_lineage_event("data_ingestion", "START")

    try:
        source_path = Path("/content/valid_books.csv")

        if not source_path.exists():
            raise FileNotFoundError(
                "Missing /content/valid_books.csv. Run the validation cells first."
            )

        books = pd.read_csv(source_path)

        missing_columns = [
            column for column in REQUIRED_COLUMNS
            if column not in books.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Source data is missing required columns: {missing_columns}"
            )

        if books.empty:
            raise ValueError("The validated books dataset is empty.")

        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda value: json.dumps(
                value,
                default=str,
            ).encode("utf-8"),
        )

        for record in books.to_dict(orient="records"):
            producer.send("books", value=record)

        producer.flush()
        producer.close()

        print(
            f"Data ingestion completed: "
            f"{len(books)} records sent to Kafka topic 'books'."
        )

        emit_lineage_event("data_ingestion", "COMPLETE", run_id)
        return len(books)

    except Exception:
        emit_lineage_event("data_ingestion", "FAIL", run_id)
        raise


def delta_lakehouse_task():
    """
    Real lakehouse task:
    Rebuilds Bronze, Silver, and Gold Delta tables from the validated dataset.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, current_timestamp
    from delta import configure_spark_with_delta_pip

    run_id = emit_lineage_event("delta_lakehouse", "START")

    try:
        builder = (
            SparkSession.builder
            .appName("SDAIA Books Airflow Lakehouse")
            .master("local[*]")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        source_path = "/content/valid_books.csv"
        bronze_path = "/content/delta/bronze"
        silver_path = "/content/delta/silver"
        gold_path = "/content/delta/gold"

        bronze_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(source_path)
            .withColumn("ingestion_time", current_timestamp())
        )

        if bronze_df.count() == 0:
            raise ValueError("Bronze input contains no records.")

        (
            bronze_df.write
            .format("delta")
            .mode("overwrite")
            .save(bronze_path)
        )

        silver_df = (
            spark.read
            .format("delta")
            .load(bronze_path)
            .dropDuplicates(["isbn"])
            .withColumn(
                "publication_year",
                col("publication_year").cast("int"),
            )
            .withColumn(
                "submission_date",
                col("submission_date").cast("date"),
            )
        )

        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .save(silver_path)
        )

        gold_df = (
            silver_df
            .groupBy("category", "language")
            .agg(count("*").alias("number_of_books"))
        )

        (
            gold_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )

        bronze_count = bronze_df.count()
        silver_count = silver_df.count()
        gold_count = gold_df.count()

        if bronze_count < silver_count:
            raise ValueError(
                "Silver record count cannot exceed Bronze record count."
            )

        if gold_count == 0:
            raise ValueError("Gold layer was created without records.")

        print(
            "Delta Lakehouse completed: "
            f"Bronze={bronze_count}, "
            f"Silver={silver_count}, "
            f"Gold={gold_count}."
        )

        spark.stop()
        emit_lineage_event("delta_lakehouse", "COMPLETE", run_id)

        return {
            "bronze": bronze_count,
            "silver": silver_count,
            "gold": gold_count,
        }

    except Exception:
        emit_lineage_event("delta_lakehouse", "FAIL", run_id)
        raise


def quality_gate_task():
    """
    Real quality gate:
    Reads the Great Expectations decision produced by the notebook.
    Failure raises an exception and blocks the RAG task.
    """
    run_id = emit_lineage_event("quality_gate", "START")

    try:
        result_path = Path("/content/quality_gate_result.json")

        if not result_path.exists():
            raise FileNotFoundError(
                "Missing quality_gate_result.json. "
                "Run the Great Expectations cells first."
            )

        with result_path.open("r", encoding="utf-8") as f:
            result = json.load(f)

        required_keys = {
            "all_passed",
            "checks_passed",
            "total_checks",
        }

        if not required_keys.issubset(result):
            raise ValueError(
                "The quality gate result file has an invalid structure."
            )

        if not result["all_passed"]:
            raise ValueError(
                "Quality Gate failed: "
                f"{result['checks_passed']} of "
                f"{result['total_checks']} checks passed."
            )

        print(
            "Quality Gate passed: "
            f"{result['checks_passed']} of "
            f"{result['total_checks']} checks passed."
        )

        emit_lineage_event("quality_gate", "COMPLETE", run_id)
        return result

    except Exception:
        emit_lineage_event("quality_gate", "FAIL", run_id)
        raise


def rag_pipeline_task():
    """
    Real RAG task:
    1. Reads the Gold Delta table.
    2. Builds source documents and overlapping chunks.
    3. Creates embeddings and a Chroma vector collection.
    4. Builds a BM25 index.
    5. Runs hybrid retrieval and cross-encoder reranking.
    6. Saves an answer with source citations.
    """
    import numpy as np
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    from sentence_transformers import SentenceTransformer, CrossEncoder
    from rank_bm25 import BM25Okapi
    import chromadb

    run_id = emit_lineage_event("rag_pipeline", "START")

    try:
        builder = (
            SparkSession.builder
            .appName("SDAIA Books Airflow RAG")
            .master("local[*]")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        gold_df = (
            spark.read
            .format("delta")
            .load("/content/delta/gold")
        )

        rows = gold_df.collect()

        if not rows:
            raise ValueError("Gold layer is empty; RAG cannot be built.")

        documents = []
        metadata = []

        for index, row in enumerate(rows):
            text = (
                f"The {row['category']} category contains "
                f"{row['number_of_books']} books in "
                f"{row['language']}."
            )

            documents.append(text)
            metadata.append({
                "doc_id": f"doc_{index}",
                "category": str(row["category"]),
                "language": str(row["language"]),
                "number_of_books": int(row["number_of_books"]),
            })

        def chunk_text(text, chunk_size=20, overlap=5):
            words = text.split()
            chunks = []
            start = 0

            while start < len(words):
                end = start + chunk_size
                chunks.append(" ".join(words[start:end]))

                if end >= len(words):
                    break

                start += chunk_size - overlap

            return chunks

        chunk_texts = []
        chunk_ids = []
        chunk_metadata = []

        for document, source in zip(documents, metadata):
            for chunk_index, chunk in enumerate(chunk_text(document)):
                chunk_texts.append(chunk)
                chunk_ids.append(
                    f"{source['doc_id']}_chunk_{chunk_index}"
                )
                chunk_metadata.append(source)

        embedding_model = SentenceTransformer(
            "all-MiniLM-L6-v2"
        )

        embeddings = embedding_model.encode(
            chunk_texts,
            normalize_embeddings=True,
        )

        client = chromadb.EphemeralClient()
        collection = client.get_or_create_collection(
            name="sdaia_books_airflow_rag"
        )

        collection.add(
            ids=chunk_ids,
            documents=chunk_texts,
            metadatas=chunk_metadata,
            embeddings=embeddings.tolist(),
        )

        bm25 = BM25Okapi(
            [chunk.lower().split() for chunk in chunk_texts]
        )

        query = (
            "Which book categories and languages "
            "are available in the library?"
        )

        query_embedding = embedding_model.encode(
            [query],
            normalize_embeddings=True,
        )[0]

        vector_results = collection.query(
            query_embeddings=[query_embedding.tolist()],
            n_results=min(5, len(chunk_texts)),
        )

        vector_documents = vector_results["documents"][0]

        bm25_scores = bm25.get_scores(query.lower().split())
        bm25_indices = np.argsort(bm25_scores)[::-1][
            :min(5, len(chunk_texts))
        ]
        bm25_documents = [
            chunk_texts[index] for index in bm25_indices
        ]

        rrf_scores = {}

        for rank, document in enumerate(vector_documents):
            rrf_scores[document] = (
                rrf_scores.get(document, 0)
                + 1 / (60 + rank + 1)
            )

        for rank, document in enumerate(bm25_documents):
            rrf_scores[document] = (
                rrf_scores.get(document, 0)
                + 1 / (60 + rank + 1)
            )

        fused_documents = sorted(
            rrf_scores,
            key=rrf_scores.get,
            reverse=True,
        )

        reranker = CrossEncoder(
            "cross-encoder/ms-marco-MiniLM-L-6-v2"
        )

        rerank_scores = reranker.predict([
            [query, document]
            for document in fused_documents
        ])

        ranked_indices = np.argsort(rerank_scores)[::-1]
        final_documents = [
            fused_documents[index]
            for index in ranked_indices[:3]
        ]

        citations = []

        for document in final_documents:
            chunk_index = chunk_texts.index(document)
            source = chunk_metadata[chunk_index]
            citations.append({
                "document": document,
                "source": source,
            })

        result = {
            "query": query,
            "answer": " ".join(final_documents),
            "citations": citations,
            "chunks_indexed": len(chunk_texts),
        }

        output_path = Path("/content/rag_airflow_result.json")

        with output_path.open("w", encoding="utf-8") as f:
            json.dump(
                result,
                f,
                ensure_ascii=False,
                indent=2,
            )

        print(
            f"RAG pipeline completed with "
            f"{len(chunk_texts)} indexed chunks."
        )
        print(f"Result saved to {output_path}")

        spark.stop()
        emit_lineage_event("rag_pipeline", "COMPLETE", run_id)

        return str(output_path)

    except Exception:
        emit_lineage_event("rag_pipeline", "FAIL", run_id)
        raise


with DAG(
    dag_id="sdaia_books_pipeline",
    description=(
        "Runs Kafka ingestion, Delta Lakehouse, "
        "Great Expectations gate, and RAG."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["SDAIA", "Kafka", "Delta", "RAG"],
) as dag:

    start = EmptyOperator(task_id="start")

    data_ingestion = PythonOperator(
        task_id="data_ingestion",
        python_callable=data_ingestion_task,
    )

    delta_lakehouse = PythonOperator(
        task_id="delta_lakehouse",
        python_callable=delta_lakehouse_task,
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate_task,
    )

    rag_pipeline = PythonOperator(
        task_id="rag_pipeline",
        python_callable=rag_pipeline_task,
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> data_ingestion
        >> delta_lakehouse
        >> quality_gate
        >> rag_pipeline
        >> end
    )
