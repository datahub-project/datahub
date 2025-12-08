"""
Utilities to create kNN-enabled OpenSearch index copies (per entity),
augmenting mappings with a multi-model-ready nested `embeddings` structure.

This does not generate or backfill embeddings.

Usage (CLI):
  uv run python semantic-search-poc/create_semantic_indices.py \
    --host localhost --port 9200 \
    --indices datasetindex_v2 chartindex_v2 dashboardindex_v2 \
    --suffix _semantic \
    --models cohere_embed_v3:1024 titan_embed_v2:1024 \
    --copy-docs false
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import argparse
import sys

from opensearchpy import OpenSearch


@dataclass(frozen=True)
class EmbeddingModelSpec:
    """Specification for an embedding model mapping to add under `embeddings`.

    Attributes
    ----------
    key: str
        The model key used in the document mapping, e.g. "cohere_embed_v3".
    dimensions: int
        Vector dimensionality for the model's embeddings.
    space_type: str
        Similarity space used by the kNN vector field. Typically "cosinesimil".
    hnsw_m: int
        HNSW M parameter for graph connectivity during indexing.
    hnsw_ef_construction: int
        HNSW ef_construction parameter for index-time accuracy/speed tradeoff.
    engine: str
        Engine implementation to use. Defaults to "lucene" on OpenSearch 2.x.
    """

    key: str
    dimensions: int
    space_type: str = "cosinesimil"
    hnsw_m: int = 16
    hnsw_ef_construction: int = 128
    engine: str = "lucene"


def _build_embeddings_properties(models: Sequence[EmbeddingModelSpec]) -> Dict[str, Any]:
    """Build the `embeddings` mapping subtree for the provided models.

    The structure follows the tech spec: nested chunks with a `knn_vector` and
    metadata fields, plus model-level summary fields.
    """

    embeddings_props: Dict[str, Any] = {}
    for spec in models:
        embeddings_props[spec.key] = {
            "properties": {
                "chunks": {
                    "type": "nested",
                    "properties": {
                        "vector": {
                            "type": "knn_vector",
                            "dimension": spec.dimensions,
                            "method": {
                                "name": "hnsw",
                                "engine": spec.engine,
                                "space_type": spec.space_type,
                                "parameters": {
                                    "m": spec.hnsw_m,
                                    "ef_construction": spec.hnsw_ef_construction,
                                },
                            },
                        },
                        "text": {"type": "text", "index": False},
                        "position": {"type": "integer"},
                        "character_offset": {"type": "integer"},
                        "character_length": {"type": "integer"},
                        "token_count": {"type": "integer"},
                    },
                },
                "total_chunks": {"type": "integer"},
                "total_tokens": {"type": "integer"},
                "model_version": {"type": "keyword"},
                "generated_at": {"type": "date"},
                "chunking_strategy": {"type": "keyword"},
            }
        }

    return embeddings_props


def _merge_embeddings_into_properties(
    base_properties: Mapping[str, Any], models: Sequence[EmbeddingModelSpec]
) -> Dict[str, Any]:
    """Return a new properties object with `embeddings` merged/extended.

    If `embeddings` already exists, its subkeys are augmented. Otherwise, a new
    `embeddings` object is created.
    """

    merged: Dict[str, Any] = dict(base_properties or {})

    existing_embeddings: Optional[Dict[str, Any]] = None
    if "embeddings" in merged and isinstance(merged["embeddings"], dict):
        existing_embeddings = merged["embeddings"].get("properties")

    new_embeddings_props = _build_embeddings_properties(models)

    if existing_embeddings is None:
        merged["embeddings"] = {"properties": new_embeddings_props}
    else:
        extended = dict(existing_embeddings)
        extended.update(new_embeddings_props)
        merged["embeddings"] = {"properties": extended}

    return merged


def _extract_source_settings_for_copy(source_settings: Mapping[str, Any]) -> Dict[str, Any]:
    """Prepare a minimal, safe-to-create index settings object.

    Copies analysis settings and common index-level knobs, stripping non-copyable
    attributes (uuid, creation_date, version, etc.). Also enables kNN.
    """

    # Source can be nested under {index_name: {settings: {...}}}
    if "settings" in source_settings:
        raw = source_settings["settings"]
    else:
        raw = source_settings

    index_section: Dict[str, Any] = dict(raw.get("index", {}))

    for key in (
        "uuid",
        "version",
        "provided_name",
        "creation_date",
        "routing",
        "hidden",
    ):
        index_section.pop(key, None)

    # Remove codec overrides to avoid conflicts with kNN codecs
    # e.g., opensearch-custom-codecs vs kNN plugin
    index_section.pop("codec", None)

    # Ensure analysis exists if present
    analysis = raw.get("analysis") or index_section.get("analysis")
    if analysis is not None:
        index_section["analysis"] = analysis

    # Enable kNN
    index_section.setdefault("knn", True)

    # Build new settings
    return {"index": index_section}


def _extract_source_properties(source_mapping: Mapping[str, Any]) -> Dict[str, Any]:
    """Fetch the `properties` object from a typical OpenSearch mapping response."""

    mappings = source_mapping.get("mappings") or source_mapping
    return dict(mappings.get("properties", {}))


def _build_target_mapping_with_embeddings(
    source_mapping: Mapping[str, Any], models: Sequence[EmbeddingModelSpec]
) -> Dict[str, Any]:
    """Produce a target `mappings` object with `embeddings` merged into properties."""

    base_properties = _extract_source_properties(source_mapping)
    merged_properties = _merge_embeddings_into_properties(base_properties, models)
    return {"properties": merged_properties}


def create_semantic_index_copy(
    client: OpenSearch,
    source_index: str,
    target_index: str,
    models: Sequence[EmbeddingModelSpec],
    copy_documents: bool = False,
    force_recreate: bool = False,
) -> Tuple[bool, str]:
    """Create a kNN-enabled copy of `source_index` as `target_index`.

    Parameters
    ----------
    client: OpenSearch
        An initialized OpenSearch client.
    source_index: str
        Name of the existing index to copy settings/mappings from.
    target_index: str
        Name of the index to create with kNN and `embeddings` mapping added.
    models: Sequence[EmbeddingModelSpec]
        Embedding model specs to add beneath `embeddings`.
    copy_documents: bool
        If True, reindex documents from source to target after creation.
    force_recreate: bool
        If True and the target index already exists, delete it first.

    Returns
    -------
    (created, message): Tuple[bool, str]
        Whether the index was created and a short message.
    """

    if client.indices.exists(index=target_index):
        if not force_recreate:
            return False, f"Index already exists: {target_index}"
        client.indices.delete(index=target_index, ignore=[404])

    source_settings = client.indices.get_settings(index=source_index)
    source_mapping = client.indices.get_mapping(index=source_index)

    # Extract nested by index name where applicable
    settings_payload = _extract_source_settings_for_copy(source_settings.get(source_index, {}))
    mapping_payload = _build_target_mapping_with_embeddings(
        source_mapping.get(source_index, {}), models
    )

    body = {"settings": settings_payload, "mappings": mapping_payload}
    client.indices.create(index=target_index, body=body)

    if copy_documents:
        reindex_body = {"source": {"index": source_index}, "dest": {"index": target_index}}
        client.reindex(body=reindex_body, refresh=True, wait_for_completion=True, request_timeout=600)

    return True, f"Created index {target_index} (kNN enabled)"


def _parse_models_arg(models: Sequence[str]) -> List[EmbeddingModelSpec]:
    """Parse CLI `--models` arguments like ["cohere_embed_v3:1024", "titan_embed_v2:1024"]."""

    parsed: List[EmbeddingModelSpec] = []
    for item in models:
        key, _, dim_str = item.partition(":")
        if not key or not dim_str:
            raise ValueError(f"Invalid model spec: {item}. Expected format key:dims")
        dims = int(dim_str)
        parsed.append(EmbeddingModelSpec(key=key, dimensions=dims))
    return parsed


def _default_indices() -> List[str]:
    """Provide a sensible default set of indices to operate on."""

    return [
        "datasetindex_v2",
        "chartindex_v2",
        "dashboardindex_v2",
        "corpuserindex_v2",
        "domainindex_v2",
        "tagindex_v2",
    ]


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Create kNN-enabled index copies with embeddings")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9200)
    parser.add_argument("--username", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument(
        "--indices",
        nargs="*",
        default=None,
        help="List of source indices. Defaults to a common subset if omitted.",
    )
    parser.add_argument(
        "--suffix",
        default="_semantic",
        help="Suffix appended to source index names to form target names.",
    )
    parser.add_argument(
        "--models",
        nargs="*",
        default=["cohere_embed_v3:1024"],
        help="Embedding models as key:dims pairs (e.g., cohere_embed_v3:1024)",
    )
    parser.add_argument(
        "--copy-docs",
        dest="copy_docs",
        default=False,
        action="store_true",
        help="Reindex documents from source to target after creation.",
    )
    parser.add_argument(
        "--force",
        dest="force",
        default=False,
        action="store_true",
        help="Delete target index if it exists and recreate it.",
    )

    args = parser.parse_args(argv)

    client = OpenSearch(
        hosts=[{"host": args.host, "port": args.port}],
        http_auth=(args.username, args.password) if args.username and args.password else None,
        use_ssl=False,
        verify_certs=False,
        ssl_show_warn=False,
    )

    if not client.ping():
        print(f"Failed to connect to OpenSearch at {args.host}:{args.port}")
        return 2

    sources = args.indices or _default_indices()
    model_specs = _parse_models_arg(args.models)

    any_created = False
    for src in sources:
        tgt = f"{src}{args.suffix}"
        created, msg = create_semantic_index_copy(
            client=client,
            source_index=src,
            target_index=tgt,
            models=model_specs,
            copy_documents=args.copy_docs,
            force_recreate=args.force,
        )
        print(msg)
        any_created = any_created or created

    return 0 if any_created else 0


if __name__ == "__main__":
    sys.exit(main())


