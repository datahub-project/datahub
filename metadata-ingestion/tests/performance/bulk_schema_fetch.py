import logging
import os
import sys

os.environ["DATAHUB_REST_EMITTER_DEFAULT_RETRY_MAX_TIMES"] = "1"

from datahub.ingestion.graph.client import get_default_graph

logging.basicConfig(level=logging.INFO)
logging.getLogger("datahub").setLevel(logging.DEBUG)

if __name__ == "__main__":
    graph = get_default_graph()
    platform = sys.argv[1]

    resolver = graph.initialize_schema_resolver_from_datahub(
        platform=platform,
        platform_instance=None,
        env="PROD",
        # batch_size=1000,
    )
