import datahub.emitter.mce_convenience as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DatasetLineageTypeClass, SchemaFieldClass,StringTypeClass

# notes = builder.make_institutionalmemory_mce(
#     datset_urn=builder.make_dataset_urn('mpp', 'greendataset'),
#     input_url=["www.ipswich.com"],
#     input_description=["GTM"],
#     actor = 'user1'
# )
# notes = builder.make_dataset_description_mce(
#     dataset_name = builder.make_dataset_urn('mpp', 'greendata'),
#     description = "this is a long and short description",
#     externalUrl = "https://yahoo.com",
#     tags = ["irrelavent", "testing"]
#     )

# fieldPath: str,
# type: "SchemaFieldDataTypeClass",
# nativeDataType: str,
# jsonPath: Union[None, str]=None,
# nullable: Optional[bool]=None,
# description: Union[None, str]=None,
# recursive: Optional[bool]=None,
# globalTags: Union[None, "GlobalTagsClass"]=None,
# glossaryTerms: Union[None, "GlossaryTermsClass"]=None,
# fieldPath: str,
#         type: "SchemaFieldDataTypeClass",
#         nativeDataType: str,
field = SchemaFieldClass(fieldPath="column1", 
                type=StringTypeClass(),
                nativeDataType="Long Text",
                nullable= True,
                description="column1 descriptor",
                )
print(field)
notes = builder.make_schema_mce(
    datset_urn = builder.make_dataset_urn('mpp', 'greendata'),
    schemaName="",
    platformName= "mpp",
    actor = "user1",
    fields = [{"fieldPath":"column1", 
                "type":"string",
                "nativeDataType":"Long Text",
                "nullable": True,
                "description":"column1 descriptor",
                },
            ],
    primaryKeys = None   
)


emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mce(notes)
