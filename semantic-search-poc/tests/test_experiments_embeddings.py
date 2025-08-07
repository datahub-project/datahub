"""Experimental parity test: Bedrock Cohere vs direct Cohere embeddings.

This test uses hardcoded texts from PET_STATUS_HISTORY and a fixed set of
queries to compare cosine similarities produced by:

- Bedrock Cohere (model: cohere.embed-english-v3)
- Direct Cohere API (model: embed-english-v3.0)

It prints a side-by-side table for manual inspection. The test gracefully
skips if the required credentials are not present.
"""

from __future__ import annotations

import os
import math
from typing import List, Tuple

import numpy as np
import pytest
from dotenv import load_dotenv

from embedding_utils import create_bedrock_embeddings, create_cohere_embeddings


def _cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    a = np.array(vec1)
    b = np.array(vec2)
    denom = (np.linalg.norm(a) * np.linalg.norm(b))
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


def _print_row(cells: List[str], widths: List[int]) -> None:
    line = "| " + " | ".join(cell.ljust(w) for cell, w in zip(cells, widths)) + " |"
    print(line)


def _print_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    _print_row(headers, widths)
    _print_row(["-" * w for w in widths], widths)
    for r in rows:
        _print_row(r, widths)


@pytest.mark.experiment
def test_bedrock_vs_cohere_pet_status_history() -> None:
    # Load env from project root and test directory
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    load_dotenv()

    # Hardcoded PET_STATUS_HISTORY Natural V2 text
    doc_text = (
        'Incremental table containing all historical statuses of a pet. '
        'The "pet_status_history" table. Contains fields: "profile_id", "status", "as_of_date". '
        'Part of the Pet Adoptions domain. Maintained by shannon@longtail.com. '
        'This is a production-ready model. This is a business-critical dataset. '
        'Uses incremental materialization and built with SQL. '
        'Glossary terms: 9afa9a59-93b2-47cb-9094-aa342eec24ad. Tagged as: "prod_model". '
        'Stored in dbt. Additional metadata - account_id: 107298, node_type: "model", '
        'contains_pii: False, project_id: 241624, job_id: 279117, catalog_type: "BASE TABLE", '
        'dbt_unique_id: "model.long_tail_companions.pet_status_history", glossary_term: "return_rate.md"'
    )

    queries = [
        "Find pet adoption records and status",
        "Show animal health and medical history",
        "Where are pet profiles and characteristics stored?",
        "Pet availability and adoption metrics",
        "Track pet status changes over time",
        "Historical pet adoption data",
    ]

    # Validate credentials presence; skip if missing
    missing = []
    if not os.getenv("COHERE_API_KEY"):
        missing.append("COHERE_API_KEY")
    if missing:
        pytest.skip(f"Missing credentials for direct Cohere: {', '.join(missing)}")

    # Instantiate embeddings directly (no cache)
    bedrock_region = os.getenv("AWS_REGION", "us-west-2")
    bedrock_model_id = os.getenv("BEDROCK_MODEL", "cohere.embed-english-v3")

    bedrock = create_bedrock_embeddings(model_id=bedrock_model_id, region=bedrock_region)
    direct = create_cohere_embeddings(model="embed-english-v3.0")

    # Compute embeddings
    bedrock_doc = bedrock.embed_documents([doc_text])[0]
    direct_doc = direct.embed_documents([doc_text])[0]

    bedrock_q = [bedrock.embed_query(q) for q in queries]
    direct_q = [direct.embed_query(q) for q in queries]

    # Sanity: dimensions should match (1024 for Cohere v3)
    assert len(bedrock_doc) == len(direct_doc) == 1024

    # Compute cosine similarities
    bedrock_sims: List[float] = [_cosine_similarity(qv, bedrock_doc) for qv in bedrock_q]
    direct_sims: List[float] = [_cosine_similarity(qv, direct_doc) for qv in direct_q]

    # Print side-by-side table
    rows: List[List[str]] = []
    for q, s_b, s_d in zip(queries, bedrock_sims, direct_sims):
        rows.append([
            q,
            f"{s_b:.3f}",
            f"{s_d:.3f}",
            f"{(s_b - s_d):+.3f}",
        ])

    print("\nBedrock vs Direct Cohere cosine similarities (PET_STATUS_HISTORY)")
    _print_table(["Query", "Bedrock", "Direct", "Delta (B-D)"], rows)

    # Optional loose checks (non-fatal): same best-3 queries by rank should overlap at least once
    bedrock_top = sorted(range(len(bedrock_sims)), key=lambda i: bedrock_sims[i], reverse=True)[:3]
    direct_top = sorted(range(len(direct_sims)), key=lambda i: direct_sims[i], reverse=True)[:3]
    overlap = set(bedrock_top) & set(direct_top)
    assert len(overlap) >= 1, "Top-3 queries have no overlap between Bedrock Cohere and direct Cohere"


@pytest.mark.experiment
def test_vector_stats_bedrock_vs_cohere_pet_status_history() -> None:
    # Load env and prepare inputs
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    load_dotenv()

    doc_text = (
        'Incremental table containing all historical statuses of a pet. '
        'The "pet_status_history" table. Contains fields: "profile_id", "status", "as_of_date". '
        'Part of the Pet Adoptions domain. Maintained by shannon@longtail.com. '
        'This is a production-ready model. This is a business-critical dataset. '
        'Uses incremental materialization and built with SQL. '
        'Glossary terms: 9afa9a59-93b2-47cb-9094-aa342eec24ad. Tagged as: "prod_model". '
        'Stored in dbt. Additional metadata - account_id: 107298, node_type: "model", '
        'contains_pii: False, project_id: 241624, job_id: 279117, catalog_type: "BASE TABLE", '
        'dbt_unique_id: "model.long_tail_companions.pet_status_history", glossary_term: "return_rate.md"'
    )

    queries = [
        "Find pet adoption records and status",
        "Show animal health and medical history",
        "Where are pet profiles and characteristics stored?",
        "Pet availability and adoption metrics",
        "Track pet status changes over time",
        "Historical pet adoption data",
    ]

    if not os.getenv("COHERE_API_KEY"):
        pytest.skip("Missing COHERE_API_KEY for direct Cohere comparison")

    bedrock_region = os.getenv("AWS_REGION", "us-west-2")
    bedrock_model_id = os.getenv("BEDROCK_MODEL", "cohere.embed-english-v3")

    bedrock = create_bedrock_embeddings(model_id=bedrock_model_id, region=bedrock_region)
    direct = create_cohere_embeddings(model="embed-english-v3.0")

    # Embed doc and queries
    bedrock_doc = bedrock.embed_documents([doc_text])[0]
    direct_doc = direct.embed_documents([doc_text])[0]
    bedrock_q = [bedrock.embed_query(q) for q in queries]
    direct_q = [direct.embed_query(q) for q in queries]

    # Helper lambda
    norm = lambda v: float(np.linalg.norm(np.array(v)))
    mean = lambda v: float(np.mean(np.array(v)))
    std = lambda v: float(np.std(np.array(v)))

    def cos(a, b):
        return _cosine_similarity(a, b)

    def l2(a, b):
        va, vb = np.array(a), np.array(b)
        return float(np.linalg.norm(va - vb))

    # Print document stats
    print("\nDocument vector stats (Bedrock vs Direct Cohere)")
    headers = [
        "Type",
        "Bedrock norm",
        "Direct norm",
        "Bedrock mean",
        "Direct mean",
        "Bedrock std",
        "Direct std",
        "cos(B,D)",
        "L2(B-D)",
    ]
    doc_row = [
        "doc",
        f"{norm(bedrock_doc):.4f}",
        f"{norm(direct_doc):.4f}",
        f"{mean(bedrock_doc):.4f}",
        f"{mean(direct_doc):.4f}",
        f"{std(bedrock_doc):.4f}",
        f"{std(direct_doc):.4f}",
        f"{cos(bedrock_doc, direct_doc):.4f}",
        f"{l2(bedrock_doc, direct_doc):.4f}",
    ]
    _print_table(headers, [doc_row])

    # Print query stats
    print("\nPer-query vector stats (Bedrock vs Direct Cohere)")
    q_headers = [
        "Query",
        "B-norm",
        "D-norm",
        "B-mean",
        "D-mean",
        "B-std",
        "D-std",
        "cos(B,D)",
        "L2(B-D)",
    ]
    q_rows: List[List[str]] = []
    for q, qb, qd in zip(queries, bedrock_q, direct_q):
        q_rows.append([
            q,
            f"{norm(qb):.4f}",
            f"{norm(qd):.4f}",
            f"{mean(qb):.4f}",
            f"{mean(qd):.4f}",
            f"{std(qb):.4f}",
            f"{std(qd):.4f}",
            f"{cos(qb, qd):.4f}",
            f"{l2(qb, qd):.4f}",
        ])
    _print_table(q_headers, q_rows)


@pytest.mark.experiment
def test_input_type_truncate_sweep() -> None:
    """Sweep input_type and truncate consistently across providers and compare."""
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    load_dotenv()

    if not os.getenv("COHERE_API_KEY"):
        pytest.skip("Missing COHERE_API_KEY for direct Cohere comparison")

    doc_text = (
        'Incremental table containing all historical statuses of a pet. '
        'The "pet_status_history" table. Contains fields: "profile_id", "status", "as_of_date". '
        'Part of the Pet Adoptions domain. Maintained by shannon@longtail.com.'
    )
    queries = [
        "Find pet adoption records and status",
        "Track pet status changes over time",
    ]

    bedrock_region = os.getenv("AWS_REGION", "us-west-2")
    bedrock_model_id = os.getenv("BEDROCK_MODEL", "cohere.embed-english-v3")

    cases = [
        ("A: doc=document, query=query", None),
        ("B: both=document", "search_document"),
        ("C: both=query", "search_query"),
        # ("D: omit input_type", "OMIT"),  # Not possible with current adapter (always sets input_type)
    ]

    for label, override in cases:
        bedrock = create_bedrock_embeddings(model_id=bedrock_model_id, region=bedrock_region)
        direct = create_cohere_embeddings(model="embed-english-v3.0")

        # Force model_id on bedrock to avoid any provider default switching
        try:
            bedrock._default_kwargs["model_id"] = bedrock_model_id
        except Exception:
            pass

        # Force truncate=NONE on both
        try:
            bedrock._default_kwargs["truncate"] = "NONE"
            direct._default_kwargs["truncate"] = "NONE"
        except Exception:
            pass

        # Optional override of input_type for both doc and query calls
        if override is not None:
            try:
                bedrock._default_kwargs["input_type"] = override
                direct._default_kwargs["input_type"] = override
            except Exception:
                pass
        else:
            # Ensure no forced override so adapter mapping applies (doc vs query)
            try:
                bedrock._default_kwargs.pop("input_type", None)
                direct._default_kwargs.pop("input_type", None)
            except Exception:
                pass

        # Compute similarities
        b_doc = bedrock.embed_documents([doc_text])[0]
        d_doc = direct.embed_documents([doc_text])[0]
        sims = []
        for q in queries:
            b_q = bedrock.embed_query(q)
            d_q = direct.embed_query(q)
            sims.append([
                q,
                f"{_cosine_similarity(b_q, b_doc):.3f}",
                f"{_cosine_similarity(d_q, d_doc):.3f}",
                f"{(_cosine_similarity(b_q, b_doc) - _cosine_similarity(d_q, d_doc)):+.3f}",
            ])

        print(f"\nCase {label} (truncate=NONE)")
        _print_table(["Query", "Bedrock cos", "Direct cos", "Delta (B-D)"], sims)



