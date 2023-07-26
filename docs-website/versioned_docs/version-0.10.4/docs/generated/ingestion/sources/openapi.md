---
sidebar_position: 33
title: OpenAPI
slug: /generated/ingestion/sources/openapi
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/openapi.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# OpenAPI

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability                                          | Status | Notes |
| --------------------------------------------------- | ------ | ----- |
| [Platform Instance](../../../platform-instances.md) | ❌     |       |

### CLI based Ingestion

#### Install the Plugin

The `openapi` source works out of the box with `acryl-datahub`.

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: openapi
  config:
    name: test_endpoint # this name will appear in DatHub
    url: https://test_endpoint.com/
    swagger_file: classicapi/doc/swagger.json # where to search for the OpenApi definitions
    get_token: # optional, if you need to get an authentication token beforehand
      request_type: get
      url: api/authentication/login?username={username}&password={password}
    username: your_username # optional
    password: your_password # optional
    forced_examples: # optionals
      /accounts/groupname/{name}: ["test"]
      /accounts/username/{name}: ["test"]
    ignore_endpoints: [/ignore/this, /ignore/that, /also/that_other] # optional, the endpoints to ignore

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                  | Description                                                                                       |
| :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------ |
| <div className="path-line"><span className="path-main">name</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>         |                                                                                                   |
| <div className="path-line"><span className="path-main">swagger_file</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |                                                                                                   |
| <div className="path-line"><span className="path-main">url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>          |                                                                                                   |
| <div className="path-line"><span className="path-main">forced_examples</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                    | <div className="default-line ">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">get_token</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                          | <div className="default-line ">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">ignore_endpoints</span></div> <div className="type-name-line"><span className="type-name">array(object)</span></div>                            |                                                                                                   |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                           | <div className="default-line ">Default: <span className="default-value"></span></div>             |
| <div className="path-line"><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                              |                                                                                                   |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                           | <div className="default-line ">Default: <span className="default-value"></span></div>             |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "OpenApiConfig",
  "type": "object",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "url": {
      "title": "Url",
      "type": "string"
    },
    "swagger_file": {
      "title": "Swagger File",
      "type": "string"
    },
    "ignore_endpoints": {
      "title": "Ignore Endpoints",
      "default": [],
      "type": "array",
      "items": {}
    },
    "username": {
      "title": "Username",
      "default": "",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "default": "",
      "type": "string"
    },
    "forced_examples": {
      "title": "Forced Examples",
      "default": {},
      "type": "object"
    },
    "token": {
      "title": "Token",
      "type": "string"
    },
    "get_token": {
      "title": "Get Token",
      "default": {},
      "type": "object"
    }
  },
  "required": [
    "name",
    "url",
    "swagger_file"
  ],
  "additionalProperties": false
}
```

</TabItem>
</Tabs>

The dataset metadata should be defined directly in the Swagger file, section `["example"]`. If this is not true, the following procedures will take place.

## Capabilities

The plugin read the swagger file where the endopints are defined and searches for the ones which accept
a `GET` call: those are the ones supposed to give back the datasets.

For every selected endpoint defined in the `paths` section,
the tool searches whether the medatada are already defined in there.
As example, if in your swagger file there is the `/api/users/` defined as follows:

```yaml
paths:
  /api/users/:
    get:
      tags: ["Users"]
      operationID: GetUsers
      description: Retrieve users data
      responses:
        "200":
          description: Return the list of users
          content:
            application/json:
              example:
                {
                  "user": "username",
                  "name": "Full Name",
                  "job": "any",
                  "is_active": True,
                }
```

then this plugin has all the information needed to create the dataset in DataHub.

In case there is no example defined, the plugin will try to get the metadata directly from the endpoint.
So, if in your swagger file you have

```yaml
paths:
  /colors/:
    get:
      tags: ["Colors"]
      operationID: GetDefinedColors
      description: Retrieve colors
      responses:
        "200":
          description: Return the list of colors
```

the tool will make a `GET` call to `https:///test_endpoint.com/colors`
and parse the response obtained.

### Automatically recorded examples

Sometimes you can have an endpoint which wants a parameter to work, like
`https://test_endpoint.com/colors/{color}`.

Since in the OpenApi specifications the listing endpoints are specified
just before the detailed ones, in the list of the paths, you will find

    https:///test_endpoint.com/colors

defined before

    https://test_endpoint.com/colors/{color}

This plugin is set to automatically keep an example of the data given by the first URL,
which with some probability will include an example of attribute needed by the second.

So, if by calling GET to the first URL you get as response:

    {"pantone code": 100,
     "color": "yellow",
     ...}

the `"color": "yellow"` part will be used to complete the second link, which
will become:

    https://test_endpoint.com/colors/yellow

and this last URL will be called to get back the needed metadata.

### Automatic guessing of IDs

If no useful example is found, a second procedure will try to guess a numerical ID.
So if we have:

    https:///test_endpoint.com/colors/{colorID}

and there is no `colorID` example already found by the plugin,
it will try to put a number one (1) at the parameter place

    https://test_endpoint.com/colors/1

and this URL will be called to get back the needed metadata.

## Config details

### Token authentication

If this tool needs to get an access token to interrogate the endpoints, this can be requested. Two methods are available at the moment:

- 'get' : this requires username/password combination to be present in the url. Note that {username} and {password} are mandatory placeholders. They will be replaced with the true credentials at runtime. Note that username and password will be sent in the request address, so it's unsecure. If your provider allows for the other method, please go for it.
- 'post' : username and password will be inserted in the body of the POST request

In both cases, username and password are the ones defined in the configuration file.

### Getting dataset metadata from `forced_example`

Suppose you have an endpoint defined in the swagger file, but without example given, and the tool is
unable to guess the URL. In such cases you can still manually specify it in the `forced_examples` part of the
configuration file.

As example, if in your swagger file you have

```yaml
paths:
  /accounts/groupname/{name}/:
    get:
      tags: ["Groups"]
      operationID: GetGroup
      description: Retrieve group data
      responses:
        "200":
          description: Return details about the group
```

and the plugin did not found an example in its previous calls,
so the tool have no idea about what substitute to the `{name}` part.

By specifying in the configuration file

```yaml
forced_examples: # optionals
  /accounts/groupname/{name}: ["test"]
```

the plugin is able to build a correct URL, as follows:

https://test_endpoint.com/accounts/groupname/test

### Code Coordinates

- Class Name: `datahub.ingestion.source.openapi.OpenApiSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/openapi.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for OpenAPI, feel free to ping us on [our Slack](https://slack.datahubproject.io).
