"""
RAG pipeline for the SDAIA Books Platform.

Stages:
1. Read the Gold Delta table.
2. Convert Gold rows to source documents.
3. Split documents into overlapping chunks.
4. Create dense embeddings.
5. Store chunks in ChromaDB.
6. Build a BM25 keyword index.
7. Fuse dense and keyword rankings with Reciprocal Rank Fusion.
8. Rerank candidates with a CrossEncoder.
9. Return a grounded answer with a source citation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import chromadb
import numpy as np
from pyspark.sql import SparkSession
from rank_bm25 import BM25Okapi
from sentence_transformers import CrossEncoder, SentenceTransformer


GOLD_PATH = "/content/delta/gold"
DEFAULT_QUERY = "How many Arabic Data Science books are available?"


@dataclass
class RAGResult:
    question: str
    answer: str
    citation: str
    retrieved_passage: str


def chunk_text(text: str, chunk_size: int = 15, overlap: int = 5) -> list[str]:
    """Split text into overlapping word chunks."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")
    if overlap < 0 or overlap >= chunk_size:
        raise ValueError("overlap must be between 0 and chunk_size - 1.")

    words = text.split()
    chunks: list[str] = []
    start = 0

    while start < len(words):
        end = start + chunk_size
        chunks.append(" ".join(words[start:end]))

        if end >= len(words):
            break

        start += chunk_size - overlap

    return chunks


def read_gold_data(gold_path: str = GOLD_PATH):
    """Read the Gold Delta table using Spark."""
    spark = SparkSession.builder.getOrCreate()
    return spark.read.format("delta").load(gold_path)


def build_source_documents(gold_df) -> tuple[list[str], list[dict[str, Any]]]:
    """Convert Gold rows into source documents and citation metadata."""
    documents: list[str] = []
    metadata: list[dict[str, Any]] = []

    for index, row in enumerate(gold_df.collect()):
        doc_id = f"doc_{index}"
        text = (
            f"In the {row['category']} category, the Smart Library holds "
            f"{row['number_of_books']} books, most of them written in {row['language']}. "
            f"This reflects the demand and popularity of {row['category']} titles "
            f"among readers who prefer {row['language']} content. "
            f"Librarians use this data to decide which {row['category']} titles "
            f"to restock in {row['language']} for the upcoming semester."
        )

        documents.append(text)
        metadata.append(
            {
                "doc_id": doc_id,
                "category": row["category"],
                "language": row["language"],
                "number_of_books": int(row["number_of_books"]),
            }
        )

    if not documents:
        raise ValueError("The Gold layer is empty; no RAG documents were created.")

    return documents, metadata


def build_chunks(
    documents: list[str],
    metadata: list[dict[str, Any]],
    chunk_size: int = 15,
    overlap: int = 5,
) -> tuple[list[str], list[str], list[dict[str, Any]]]:
    """Create chunks, unique chunk IDs, and source metadata."""
    chunk_texts: list[str] = []
    chunk_ids: list[str] = []
    chunk_sources: list[dict[str, Any]] = []

    for document, source in zip(documents, metadata):
        for chunk_index, chunk in enumerate(
            chunk_text(document, chunk_size=chunk_size, overlap=overlap)
        ):
            chunk_texts.append(chunk)
            chunk_ids.append(f"{source['doc_id']}_chunk_{chunk_index}")
            chunk_sources.append(source)

    return chunk_texts, chunk_ids, chunk_sources


def reciprocal_rank_fusion(
    vector_documents: list[str],
    bm25_documents: list[str],
    rank_constant: int = 60,
) -> list[str]:
    """Fuse dense and keyword rankings using Reciprocal Rank Fusion."""
    scores: dict[str, float] = {}

    for rank, document in enumerate(vector_documents):
        scores[document] = scores.get(document, 0.0) + 1 / (rank_constant + rank + 1)

    for rank, document in enumerate(bm25_documents):
        scores[document] = scores.get(document, 0.0) + 1 / (rank_constant + rank + 1)

    return sorted(scores, key=scores.get, reverse=True)


def run_rag_pipeline(
    query: str = DEFAULT_QUERY,
    gold_path: str = GOLD_PATH,
    top_k: int = 5,
) -> RAGResult:
    """Execute the complete hybrid-search RAG pipeline."""
    gold_df = read_gold_data(gold_path)
    documents, metadata = build_source_documents(gold_df)
    chunk_texts, chunk_ids, chunk_sources = build_chunks(documents, metadata)

    embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = embedding_model.encode(chunk_texts).tolist()

    chroma_client = chromadb.Client()
    collection_name = "books"

    try:
        chroma_client.delete_collection(collection_name)
    except Exception:
        pass

    collection = chroma_client.create_collection(name=collection_name)
    collection.add(
        documents=chunk_texts,
        embeddings=embeddings,
        ids=chunk_ids,
        metadatas=chunk_sources,
    )

    tokenized_documents = [chunk.lower().split() for chunk in chunk_texts]
    bm25 = BM25Okapi(tokenized_documents)

    query_embedding = embedding_model.encode(query).tolist()
    result_count = min(top_k, len(chunk_texts))

    vector_results = collection.query(
        query_embeddings=[query_embedding],
        n_results=result_count,
    )
    vector_documents = vector_results["documents"][0]

    bm25_scores = bm25.get_scores(query.lower().split())
    top_indices = np.argsort(bm25_scores)[::-1][:result_count]
    bm25_documents = [chunk_texts[index] for index in top_indices]

    fused_results = reciprocal_rank_fusion(vector_documents, bm25_documents)

    cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
    pairs = [[query, document] for document in fused_results]
    rerank_scores = cross_encoder.predict(pairs)

    ranked_results = [
        document
        for _, document in sorted(
            zip(rerank_scores, fused_results),
            key=lambda item: float(item[0]),
            reverse=True,
        )
    ]

    chunk_to_metadata = dict(zip(chunk_texts, chunk_sources))
    top_chunk = ranked_results[0]
    source = chunk_to_metadata[top_chunk]

    answer = (
        f"Based on the retrieved passage, the library holds "
        f"{source['number_of_books']} books in the {source['category']} category, "
        f"mostly in {source['language']}."
    )

    citation = (
        f"[Source: {source['doc_id']} | "
        f"Category={source['category']}, Language={source['language']}]"
    )

    return RAGResult(
        question=query,
        answer=answer,
        citation=citation,
        retrieved_passage=top_chunk,
    )


def main() -> None:
    result = run_rag_pipeline()

    print("Question:")
    print(result.question)
    print("\nAnswer:")
    print(result.answer)
    print("\nCitation:")
    print(result.citation)
    print("\nRetrieved Passage:")
    print(result.retrieved_passage)


if __name__ == "__main__":
    main()
