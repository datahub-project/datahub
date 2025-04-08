# Transformer Name 

<!-- Set Support Status -->
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


## Overview

<!-- Plain-language description of what this transformer is meant to do.  -->

### Capabilities

<!-- Bulleted list of capabilities for short-form consumption -->

### Supported Events

<!-- List of Event Types which are supported vs are not. -->


## Transformer Quickstart 

### Prerequisites

In order to use [Transformer Name], you will need:

* eg. Python version, source version, source access requirements
* eg. Steps to configure access
* ...

### Install the Plugin(s)

Run the following commands to install the relevant Transformer plugin(s):

`pip install 'acryl-datahub-actions[transformer-name]'`

### Configure the Transformer Config

Use the following config(s) to get started with this Transformer. 

```yml
name: "pipeline-name"
source:
  # source configs
transformers:
  - type: "transformer-name"
    config:
      # transformer configs 
action:
  # action configs
```

<details>
  <summary>View All Configuration Options</summary>
  
  | Field | Required | Default | Description |
  | --- | :-: | :-: | --- |
  | `field1` | ✅ | `default_value` | A required field with a default value |
  | `field2` | ❌ | `default_value` | An optional field with a default value |
  | `field3` | ❌ | | An optional field without a default value |
  | ... | | |
</details>


## Troubleshooting

### [Common Issue]

[Provide description of common issues with this transformer and steps to resolve]