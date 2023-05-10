properties = {k.capitalize().replace("_", " "): v for k, v in properties.items()}

# Create properties object for change proposal wrapper
feature_table_properties = MLFeatureTablePropertiesClass(
    customProperties=properties,
    description="Description not provided." if not urn_description else urn_description,
)

# MCP creation
mcp = MetadataChangeProposalWrapper(
    entityType="mlFeatureTable",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=urn,
    aspect=feature_table_properties,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter(self.gms_endpoint)

# Emit metadata!
emitter.emit_mcp(mcp)
