# Metadata Ingestion Quickstart

## Prerequisites

In order to ingest metadata from Amplitude, you will need:

* an account in Amplitude
* at least one project within that account that contains data
* api keys and secret keys for each project to ingest

## Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[amplitude]'`

## Configure the Ingestion Recipe(s)

Use the following recipe(s) to get started with ingestion:

```yml
source:
  type: "amplitude"
  config:
    connect_uri: "https://amplitude.com/api/2"
    projects:
      - name: project name
        description: project description
        api_key: project api key
        secret_key: project secret key
sink:
  # sink configs
```

Further projects can be added in the following way:

```yaml
source:
  type: "amplitude"
  config:
    connect_uri: "https://amplitude.com/api/2"
    projects:
      - name: project name
        description: project description
        api_key: project api key
        secret_key: project secret key
      - name: project name
        description: project description
        api_key: project api key
        secret_key: project secret key
sink:
  # sink configs
```

## Integration details

This plugin leverages the
[Amplitude Taxonomy endpoint](https://www.docs.developers.amplitude.com/analytics/apis/taxonomy-api/)
to extract the following:

* Event
* Event Categories
* Event Properties
* User Properties

It maps these concepts to Datahub concepts in the following way:

| Amplitude Concept | Datahub Concept  | Notes                           |
|-------------------|------------------|---------------------------------|
| Project           | Container        | Can be multiple projects        |
| Event             | Dataset          |                                 |
| Event Category    | Dataset property |                                 |
| Event Property    | Schema Field     |                                 |
| User Property     | Schema Field     | Ingested on a per project basis |

## Current limitations

Amplitude does not currently support a project endpoint.
Any description a project has in Amplitude needs to be included in
the ingestion recipe for it to be visible in Datahub.

Event Properties and User Properties have some other attributes that while ingestible,
are not currently supported for display in the UI. These include:

* regex: Custom regex that can be used for pattern matching or more complex values
* enum_values: A predefined list of property values that are accepted
* is_array_type: Boolean on whether the value is a list of properties
* is_required: Boolean on whether the property is required
