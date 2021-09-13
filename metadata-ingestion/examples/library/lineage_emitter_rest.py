import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Construct a lineage object.
lineage_mce = builder.make_lineage_mce(
    [
        builder.make_dataset_urn("bigquery", "upstream1"),
        builder.make_dataset_urn("bigquery", "upstream2"),
    ],
    builder.make_dataset_urn("bigquery", "downstream"),
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mce(lineage_mce)
