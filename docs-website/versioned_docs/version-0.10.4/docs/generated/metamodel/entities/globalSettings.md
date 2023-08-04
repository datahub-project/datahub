---
sidebar_position: 43
title: GlobalSettings
slug: /generated/metamodel/entities/globalsettings
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/globalSettings.md
---

# GlobalSettings

Global settings for an the platform

## Aspects

### globalSettingsInfo

DataHub Global platform settings. Careful - these should not be modified by the outside world!

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "globalSettingsInfo"
  },
  "name": "GlobalSettingsInfo",
  "namespace": "com.linkedin.settings.global",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "GlobalViewsSettings",
          "namespace": "com.linkedin.settings.global",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "defaultView",
              "default": null,
              "doc": "The default View for the instance, or organization."
            }
          ],
          "doc": "Settings for DataHub Views feature."
        }
      ],
      "name": "views",
      "default": null,
      "doc": "Settings related to the Views Feature"
    }
  ],
  "doc": "DataHub Global platform settings. Careful - these should not be modified by the outside world!"
}
```

</details>

## Relationships

## [Global Metadata Model](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)
