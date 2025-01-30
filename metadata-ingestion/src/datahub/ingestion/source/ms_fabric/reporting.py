from datahub.ingestion.api.source import SourceReport


class AzureFabricSourceReport(SourceReport):
    datasets_scanned: int = 0
    lineage_edges_scanned: int = 0
    usage_events_scanned: int = 0
    jobs_scanned: int = 0
    dataflows_scanned: int = 0
