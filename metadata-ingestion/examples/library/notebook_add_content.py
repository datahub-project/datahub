# metadata-ingestion/examples/library/notebook_add_content.py
import logging
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    ChartCellClass,
    NotebookCellClass,
    NotebookCellTypeClass,
    NotebookContentClass,
    QueryCellClass,
    TextCellClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

notebook_urn = "urn:li:notebook:(querybook,customer_analysis_2024)"

audit_stamp = AuditStampClass(
    time=int(time.time() * 1000), actor="urn:li:corpuser:data_scientist"
)
change_audit = ChangeAuditStampsClass(created=audit_stamp, lastModified=audit_stamp)

cells = [
    NotebookCellClass(
        type=NotebookCellTypeClass.TEXT_CELL,
        textCell=TextCellClass(
            cellId="cell-1",
            cellTitle="Introduction",
            text="# Customer Segmentation Analysis\n\nThis notebook analyzes customer behavior patterns to identify high-value segments.",
            changeAuditStamps=change_audit,
        ),
    ),
    NotebookCellClass(
        type=NotebookCellTypeClass.QUERY_CELL,
        queryCell=QueryCellClass(
            cellId="cell-2",
            cellTitle="Customer Activity Query",
            rawQuery="SELECT customer_id, SUM(revenue) as total_revenue, COUNT(*) as order_count FROM orders WHERE order_date >= '2024-01-01' GROUP BY customer_id ORDER BY total_revenue DESC LIMIT 1000",
            lastExecuted=audit_stamp,
            changeAuditStamps=change_audit,
        ),
    ),
    NotebookCellClass(
        type=NotebookCellTypeClass.CHART_CELL,
        chartCell=ChartCellClass(
            cellId="cell-3",
            cellTitle="Revenue Distribution by Segment",
            changeAuditStamps=change_audit,
        ),
    ),
]

notebook_content = NotebookContentClass(cells=cells)

event = MetadataChangeProposalWrapper(
    entityUrn=notebook_urn,
    aspect=notebook_content,
)

emitter.emit(event)
log.info(f"Added content to notebook {notebook_urn}")
