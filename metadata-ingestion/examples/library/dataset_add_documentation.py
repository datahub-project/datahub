from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="hive", name="realestate_db.sales"))

# Add dataset documentation
documentation = """## The Real Estate Sales Dataset
This is a really important Dataset that contains all the relevant information about sales that have happened organized by address.
"""
dataset.set_description(documentation)

# Add link to institutional memory
dataset.add_link(
    (
        "https://wikipedia.com/real_estate",
        "This is the definition of what real estate means",  # link description
    )
)

client.entities.update(dataset)
