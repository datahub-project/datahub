# #!/usr/bin/env python3
import os
from aurora_client import AuroraClient
from acryl_openapi_client import AcrylOpenapiClient

ACRYL_ADD_ENTITIES_ENDPOINT = "/openapi/entities/v1/"
ACRYL_HOST = "https://zendesk-staging.acryl.io/api/gms"
ACRYL_BEARER_TOKEN = "<ENTER_ACRYL_BEARER_TOKEN>"

if __name__ == '__main__':
    print("Aurora Ingestion Initiated!")

    aurora_client = AuroraClient()
    aurora_datasets = aurora_client.get_aurora_datasets()  # Hiding this internal method to retrieve the datasets
    # Returns a List of Aurora Objects.
    # Each Aurora Object contains a unique key which is group name (folder name) & list of Schemas
    # Each of the schema needs to be uploaded to Acryl in the sructure:
    # folderName
    #   Schemas1
    #   Schemas2, and all the associated schemas related to the group

    print(f"Fetched {len(aurora_datasets)} aurora_datasets!\n Transformation Begins!!")

    # The aurora object support different data types as Acryl, so manual transformation
    # need to happen to convert the data types for the schemas
    transformed_aurora_datasets = []

    # Iterate over List of Aurora Objects
    for dataset_index, aurora_datasets_by_groups in enumerate(aurora_datasets):
        # Iterate over List of each Aurora group
        for group_dataset_index, aurora_group_dataset in enumerate(aurora_datasets_by_groups):
            dataset_group = aurora_group_dataset['dataset_group']
            dataset_schemas = aurora_group_dataset['dataset_schemas']

            print(f"\nTransforming:2 dataset_group: {dataset_group} has: {len(dataset_schemas)} "
                  f"schemas @ dataset_index: {dataset_index}; group_dataset_index: {group_dataset_index}")

            # Iterate over List of each Aurora Schema within each group
            for dataset_schema_index, dataset_schema in enumerate(dataset_schemas):
                dataset_fields = dataset_schema['fields']
                dataset_schema_name = dataset_schema['name']
                dataset_name = f"{dataset_group}.{dataset_schema_name}"

                print(f"dataset_name: {dataset_name}")
                print(
                    f"\nTransforming:2.1 dataset_schema_index: {dataset_schema_index} has dataset_schema_name: {dataset_schema_name}"
                    f" within dataset_group: {dataset_group} with: {len(dataset_fields)} fields")

                # Aurora to Acryl Data Type Conversion happens below
                transformed_acryl_dataset_schema = [aurora_client.transform_aurora_field(field) for field in
                                                    dataset_fields]

                print(
                    f"Transforming:2.2 len.transformed_acryl_dataset_schema: {len(transformed_acryl_dataset_schema)} "
                    f"\n Adding transformed_aurora_datasets to Acryl")

                acryl_openapi_client = AcrylOpenapiClient(environment=os.getenv('DATASOURCE_ENVIRONMENT', 'TEST'),
                                                          auth=ACRYL_BEARER_TOKEN, host=ACRYL_HOST, timeout=1800)

                response = acryl_openapi_client.add_dataset(endpoint=ACRYL_ADD_ENTITIES_ENDPOINT,
                                                            platform_name="aurora",
                                                            dataset_name=dataset_name,
                                                            schema=transformed_acryl_dataset_schema)

                if response == 200:
                    print(
                        f"Success: response: {response} Datacatalog Ingestion - Aurora Ingestion --- Successfully added {dataset_schema_name}")
                else:
                    print(
                        f"Error: response: {response} Datacatalog Ingestion - Aurora Ingestion --- Failure in adding {dataset_schema_name}")
