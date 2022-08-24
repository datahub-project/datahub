import logging

from datahub.ingestion.source.file import GenericFileSource

logger = logging.getLogger(__name__)


def check_mce_file(filepath: str) -> str:
    mce_source = GenericFileSource.create({"filename": filepath}, None)
    for _ in mce_source.get_workunits():
        pass
    if mce_source.get_report().failures:
        # raise the first failure found
        logger.error(
            f"Event file check failed with errors. Raising first error found. Full report {mce_source.get_report().as_string()}"
        )
        for failure_list in mce_source.get_report().failures.values():
            if len(failure_list):
                raise Exception(failure_list[0])
        raise Exception(
            f"Failed to process file due to {mce_source.get_report().failures}"
        )
    else:
        return f"{mce_source.get_report().events_produced} MCEs found - all valid"
