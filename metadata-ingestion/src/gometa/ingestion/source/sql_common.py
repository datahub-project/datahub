from sqlalchemy import create_engine
from sqlalchemy import types
from sqlalchemy.engine import reflection
from gometa.metadata.model import * 

@dataclass
class SqlWorkUnit(WorkUnit):
    mce: MetadataChangeEvent 
    

def get_schema_metadata(columns) -> SchemaMetadata:



def get_column_type(column_type):
    """
    Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
    """
    if isinstance(column_type, (types.Integer, types.Numeric)):
        return ("com.linkedin.pegasus2avro.schema.NumberType", {})

    if isinstance(column_type, (types.Boolean)):
        return ("com.linkedin.pegasus2avro.schema.BooleanType", {})

    if isinstance(column_type, (types.Enum)):
        return ("com.linkedin.pegasus2avro.schema.EnumType", {})

    if isinstance(column_type, (types._Binary, types.PickleType)):
        return ("com.linkedin.pegasus2avro.schema.BytesType", {})

    if isinstance(column_type, (types.ARRAY)):
        return ("com.linkedin.pegasus2avro.schema.ArrayType", {})

    if isinstance(column_type, (types.String)):
        return ("com.linkedin.pegasus2avro.schema.StringType", {})

    return ("com.linkedin.pegasus2avro.schema.NullType", {})

def get_sql_workunits(url, options, platform):
    engine = create_engine(url, **options)
    inspector = reflection.Inspector.from_engine(engine)
    for schema in inspector.get_schema_names():
        for table in inspector.get_table_names(schema):
            columns = inspector.get_columns(table, schema)
            mce = MetadataChangeEvent()
            dataset_snapshot = DatasetMetadataSnapshot(platform = platform, dataset_name = f'{schema}.{table}')
            schema_metadata = get_schema_metadata(columns)
            dataset_snapshot.with_aspect(schema_metadata)
            mce.with_snapshot(dataset_snapshot)
            yield SqlWorkUnit(mce = mce)
        

