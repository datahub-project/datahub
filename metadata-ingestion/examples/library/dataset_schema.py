from datahub import DataHubClient, Dataset

# Reads config from DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN, or from ~/.datahubenv.
client = DataHubClient.from_env()
# alternative: DataHubClient(server=..., token=...)

dataset = Dataset(
    platform="hive",
    name="realestate_db.sales",
    schema=[
        # tuples of (field name, data type, description)
        (
            "address.zipcode",
            "varchar(50)",
            "This is the zipcode of the address. Specified using extended form and limited to addresses in the United States",
        ),
        ("address.street", "varchar(100)", "Street corresponding to the address"),
        ("last_sold_date", "date", "Date of the last sale date for this property"),
    ],
)
client.entities.upsert(dataset)
