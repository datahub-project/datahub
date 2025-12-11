<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

If you were using `database_alias` in one of your other ingestions to rename your databases to something else based on business needs you can rename them in superset also

```yml
source:
  type: preset
  config:
    # Coordinates
    connect_uri: Preset workspace URL
    manager_uri: https://api.app.preset.io

    # Credentials
    api_key: API key
    api_secret: API secret
    database_alias:
      example_name_1: business_name_1
      example_name_2: business_name_2

sink:
  # sink configs
```
