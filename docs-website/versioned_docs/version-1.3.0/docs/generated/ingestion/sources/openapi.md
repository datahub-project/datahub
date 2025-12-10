---
sidebar_position: 49
title: OpenAPI
slug: /generated/ingestion/sources/openapi
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/openapi.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# OpenAPI
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Platform Instance](../../../platform-instances.md) | ❌ | . |



### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: openapi
  config:
    name: test_endpoint # this name will appear in DatHub
    url: https://test_endpoint.com/
    swagger_file: classicapi/doc/swagger.json  # where to search for the OpenApi definitions

    # option 1: bearer token
    bearer_token: "<token>"

    # option 2: dynamically generated tokens, username/password is mandetory
    get_token:
        request_type: get
        url_complement: api/authentication/login?username={username}&password={password}
    username: your_username
    password: your_password

    # option 3: using basic auth
    username: your_username
    password: your_password

    forced_examples:  # optionals
      /accounts/groupname/{name}: ['test']
      /accounts/username/{name}: ['test']
    ignore_endpoints: [/ignore/this, /ignore/that, /also/that_other]  # optional, the endpoints to ignore

sink:
  type: "datahub-rest"
  config:
    server: 'http://localhost:8080'
```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">name</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of ingestion.  |
| <div className="path-line"><span className="path-main">swagger_file</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Route for access to the swagger file. e.g. openapi.json  |
| <div className="path-line"><span className="path-main">url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Endpoint URL. e.g. https://example.com  |
| <div className="path-line"><span className="path-main">bearer_token</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Bearer token for endpoint authentication. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">forced_examples</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | If no example is provided for a route, it is possible to create one using forced_example. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">get_token</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Retrieving a token from the endpoint. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Password used for basic HTTP authentication. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">proxies</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | Eg. `{'http': 'http://10.10.1.10:3128', 'https': 'http://10.10.1.10:1080'}`.If authentication is required, add it to the proxy url directly e.g. `http://user:pass@10.10.1.10:3128/`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Token for endpoint authentication. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Username used for basic HTTP authentication. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">verify_ssl</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable SSL certificate verification <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ignore_endpoints</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of endpoints to ignore during ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">ignore_endpoints.</span><span className="path-main">object</span></div> <div className="type-name-line"><span className="type-name">object</span></div> |   |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "additionalProperties": false,
  "properties": {
    "name": {
      "description": "Name of ingestion.",
      "title": "Name",
      "type": "string"
    },
    "url": {
      "description": "Endpoint URL. e.g. https://example.com",
      "title": "Url",
      "type": "string"
    },
    "swagger_file": {
      "description": "Route for access to the swagger file. e.g. openapi.json",
      "title": "Swagger File",
      "type": "string"
    },
    "ignore_endpoints": {
      "default": [],
      "description": "List of endpoints to ignore during ingestion.",
      "items": {},
      "title": "Ignore Endpoints",
      "type": "array"
    },
    "username": {
      "default": "",
      "description": "Username used for basic HTTP authentication.",
      "title": "Username",
      "type": "string"
    },
    "password": {
      "default": "",
      "description": "Password used for basic HTTP authentication.",
      "title": "Password",
      "type": "string"
    },
    "proxies": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Eg. `{'http': 'http://10.10.1.10:3128', 'https': 'http://10.10.1.10:1080'}`.If authentication is required, add it to the proxy url directly e.g. `http://user:pass@10.10.1.10:3128/`.",
      "title": "Proxies"
    },
    "forced_examples": {
      "additionalProperties": true,
      "default": {},
      "description": "If no example is provided for a route, it is possible to create one using forced_example.",
      "title": "Forced Examples",
      "type": "object"
    },
    "token": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Token for endpoint authentication.",
      "title": "Token"
    },
    "bearer_token": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Bearer token for endpoint authentication.",
      "title": "Bearer Token"
    },
    "get_token": {
      "additionalProperties": true,
      "default": {},
      "description": "Retrieving a token from the endpoint.",
      "title": "Get Token",
      "type": "object"
    },
    "verify_ssl": {
      "default": true,
      "description": "Enable SSL certificate verification",
      "title": "Verify Ssl",
      "type": "boolean"
    }
  },
  "required": [
    "name",
    "url",
    "swagger_file"
  ],
  "title": "OpenApiConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>

The dataset metadata should be defined directly in the Swagger file, section `["example"]`. If this is not true, the following procedures will take place.

## Capabilities

This plugin reads the swagger file where the endpoints are defined, reads example data if provided (for any method), or searches for
data for the endpoints which do not have example data and accept a `GET` call.

For every selected endpoint defined in the `paths` section,
the tool searches whether the metadata are already defined.
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

In case there is no example defined, the plugin will try to get the metadata directly from the endpoint, if it is a `GET` method.
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

the tool will make a `GET` call to `https://test_endpoint.com/colors`
and parse the response obtained.

### Automatically recorded examples

Sometimes you can have an endpoint which wants a parameter to work, like
`https://test_endpoint.com/colors/{color}`.

Since in the OpenApi specifications the listing endpoints are specified
just before the detailed ones, in the list of the paths, you will find

    https://test_endpoint.com/colors

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

    https://test_endpoint.com/colors/{colorID}

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

and the plugin did not find an example in its previous calls,
the tool has no idea about what to substitute for the `{name}` part.

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

If you've got any questions on configuring ingestion for OpenAPI, feel free to ping us on [our Slack](https://datahub.com/slack).
