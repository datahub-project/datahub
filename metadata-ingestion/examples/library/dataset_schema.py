from datahub.sdk import DataHubClient, Dataset

client = DataHubClient.from_env()

dataset = Dataset(
    platform="hive",
    name="realestate_db.sales",
    schema=[
        # tuples of (field name / field path, data type, description)
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
