import logging

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.file import GenericFileSource

logger = logging.getLogger(__name__)


def check_mce_file(filepath: str) -> str:
    mce_source = GenericFileSource.create(
        {"filename": filepath}, PipelineContext(run_id="json-file")
    )
    for _ in mce_source.get_workunits():
        pass
    if len(mce_source.get_report().failures):
        # raise the first failure found
        logger.error(
            f"Event file check failed with errors. Raising first error found. Full report {mce_source.get_report().as_string()}"
        )
        for failure in mce_source.get_report().failures:
            raise Exception(failure.context)
        raise Exception(
            f"Failed to process file due to {mce_source.get_report().failures}"
        )
    else:
        return f"{mce_source.get_report().events_produced} MCEs found - all valid"
