## Capabilities

This module is used to allow creating datasets for Open API interfaces documented in service catalog based on [Backstage.io](https://backstage.io/).



Service catalog is used to document running services.

### Description

Loading of Open API interfaces is performed in three steps:

1. List of services is fetched from service catalog (see [Backstage.io API documentation](https://backstage.io/docs/features/software-catalog/descriptor-format)).
2. Retrieved services are checked for Open API interfaces matching [defined criteria](#criteria).
3. Ingestion performed using the **[Open API module](docs/generated/ingestion/sources/openapi/openapi.md)** is triggered for every Open API interface conforming to the [criteria](#criteria).

Swagger file is taken from the service documentation and is provided to the Open API module by its direct embedding into the Open API recipe.

No datasets are created directly by the Backstage module, the Open API module is responsible for that according to Open API properties.

### Datasets Name

Names of datasets for a particular interface are derived from the respective swagger file, in particular from attribute  `title` in section `info`. 
Attribute `title` is thus mandatory for successful creation of datasets.

### Criteria

If Open API interface matches all criteria listed below, they will be selected for ingestion.
Otherwise, they will be logged and skipped.

1. Item has value _API_ in `kind`.
2. Sections `spec` and `metadata` are not missing.
3. Value in `metadata/annotations` is not missing.
4. Value of `spec/type` is equal to _openapi_.
5. Value of `metadata/annotations/backstage.io/managed-by-location` starts with _url:_.
6. Value of `metadata/annotations/backstage.io/orphan` is _false_;
7. Value of `spec/definition` is valid Open API swagger.