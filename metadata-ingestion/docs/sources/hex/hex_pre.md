<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

### Prerequisites

#### Workspace name

Workspace name is required to fetch the data from Hex. You can find the workspace name in the URL of your Hex home page.

```
https://app.hex.tech/<workspace_name>"
```

_Eg_: In https://app.hex.tech/acryl-partnership, `acryl-partnership` is the workspace name.

#### Authentication

To authenticate with Hex, you will need to provide your Hex API Bearer token.
You can obtain your API key by following the instructions on the [Hex documentation](https://learn.hex.tech/docs/api/api-overview).

Either PAT (Personal Access Token) or Workspace Token can be used as API Bearer token:

- (Recommended) If Workspace Token, a read-only token would be enough for ingestion.
- If PAT, ingestion will be done with the user's permissions.
