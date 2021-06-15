from datahub.ingestion.source.mce_file import MetadataFileSource


def check_mce_file(filepath: str) -> str:
    mce_source = MetadataFileSource.create({"filename": filepath}, None)
    for _ in mce_source.get_workunits():
        pass
    return f"{mce_source.get_report().workunits_produced} MCEs found - all valid"
