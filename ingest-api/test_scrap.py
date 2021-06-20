
from helper.mce_convenience import make_schema_mce, make_dataset_urn, \
                    make_user_urn, make_institutionalmemory_mce, \
                    make_dataset_description_mce, make_browsepath_mce, make_ownership_mce 
from datahub.emitter.rest_emitter import DatahubRestEmitter

all_mce = []
notes1 = make_dataset_description_mce(
    dataset_name = make_dataset_urn('gooner', 'dataset1 2'),
    description = "this is a long and short description",
    externalUrl = "https://yahoo.com",
    tags = ["irrelavent", "testing"]
    )


mce = make_schema_mce(
    datset_urn = make_dataset_urn('gooner', 'dataset1 2'),
    platformName= "gooner",
    fields=[{"fieldPath": "columnA",
                "type": "string",
                "nativeType":"varchar(100)",
                "description":"column A is a pain"}, 
            {"fieldPath": "columnB",
                "type": "bool",
                "nativeType":"bool()"},
            {"fieldPath": "columnC",
                "type": "num",
                "nativeType":"numberleh"},
            {"fieldPath": "columnE",
                "type": "record",
                "nativeType":"timestamp",
                "description":"column E is a struct"},
            {"fieldPath": "columnE.shipping",
                "type": "num",
                "nativeType":"timestamp",
                "description":"subcol"},
            {"fieldPath": "columnE.shipment",
                "type": "bool",
                "nativeType":"timestamp",
                "description":"subcol"}     
            ],
    actor = "datahub",
    primaryKeys=['columnA']
)
path = make_browsepath_mce(dataset_urn = make_dataset_urn('gooner', 'dataset1 2'),
                            path = ['/custom/dataset1 2'])

owner = make_ownership_mce(dataset_urn = make_dataset_urn('gooner', 'dataset1 2'),
                                    owner = 'datahub')
all_mce.append(notes1)
all_mce.append(mce )
all_mce.append(path)
all_mce.append(owner)
# print(mce)
emitter = DatahubRestEmitter("http://localhost:8080")

for item in all_mce:
    emitter.emit_mce(item)    