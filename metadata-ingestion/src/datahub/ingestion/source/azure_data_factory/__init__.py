"""Azure Data Factory DataHub connector.

This package provides a connector to ingest metadata from Azure Data Factory
into DataHub, including:

- Data Factories as Containers
- Pipelines as DataFlows
- Activities as DataJobs
- Dataset lineage
- Execution history (optional)

Usage:
    source:
      type: azure_data_factory
      config:
        subscription_id: ${AZURE_SUBSCRIPTION_ID}
        credential:
          authentication_method: service_principal
          client_id: ${AZURE_CLIENT_ID}
          client_secret: ${AZURE_CLIENT_SECRET}
          tenant_id: ${AZURE_TENANT_ID}
"""
