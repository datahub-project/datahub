## Integration details

This plugin leverages the [Amplitude Taxonomy endpoint](https://www.docs.developers.amplitude.com/analytics/apis/taxonomy-api/) to extract the following:

- Event
- Event Categories
- Event Properties
- User Properties

It maps these concepts to Datahub concepts in the following way:

| Amplitude Concept | Datahub Concept  | Notes                                                                             |
|-------------------|------------------|-----------------------------------------------------------------------------------|
| Project           | Container        | Can be multiple projects, each defined as its own container                       |
| Event             | Dataset          |                                                                                   |
| Event Category    | Dataset property |                                                                                   |
| Event Property    | Schema Field     |                                                                                   |
| User Property     | Schema Field     | Ingested as schema fields within a User Properties dataset on a per project basis |

## Current limitations

Amplitude does not currently support a project endpoint. Any description a project has in Amplitude needs to be included
in the ingestion recipe for it to be visible in Datahub.

Event Properties and User Properties have some other attributes that while ingestible, are not currently supported for display
in the UI. These include:

- regex: Custom regex that can be used for pattern matching or more complex values
- enum_values: A predefined list of property values that are accepted 
- is_array_type: Boolean on whether the value is a list of properties
- is_required: Boolean on whether the property is required
