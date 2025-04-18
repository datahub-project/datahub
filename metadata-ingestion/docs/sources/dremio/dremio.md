### Starter Receipe for Dremio Cloud Instance 

```
source:
  type: dremio
  config:
    # Authentication details
    authentication_method: PAT        # Use Personal Access Token for authentication
    password: <your_api_token>        # Replace <your_api_token> with your Dremio Cloud API token
    is_dremio_cloud: True             # Set to True for Dremio Cloud instances
    dremio_cloud_project_id: <project_id>  # Provide the Project ID for Dremio Cloud

    # Enable query lineage tracking
    include_query_lineage: True       

    #Optional
    source_mappings:
      - platform: s3                 
        source_name: samples       
    
    # Optional
    schema_pattern:
      allow:
        - "<source_name>.<table_name>" 

sink:
    # Define your sink configuration here

```