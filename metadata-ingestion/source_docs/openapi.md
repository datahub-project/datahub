# OpenApi Metadata

This plugin is meant to gather dataset-like informations about OpenApi Endpoints.

As example, if by calling GET at the endpoint at `https://test_endpoint.com/users/by_id/1` you obtain as result:
```JSON
     {"user": "albert_physics",
      "name": "Albert Einstein",
      "job": "nature declutterer"}
```

in Datahub you will see a dataset called `test_endpoint/users` which contains as fields `user`, `name` and `job`.

## Setup

To install this plugin, run `pip install 'acryl-datahub[openapi]'`.


Example of ingestion file:

```yml
source:
  type: openapi
  config:
    name: test_endpoint # this name will appear in DatHub
    url: https://test_endpoint.com/
    swagger_file: classicapi/doc/swagger.json  # where to search for the OpenApi definitions
    get_token: True  # optional, if you need to get an authentication token beforehand 
    username: your_username  # optional
    password: your_password  # optional
    forced_examples:  # optionals
      /accounts/groupname/{name}: test
      /accounts/username/{name}: test
    ignore_endpoints: [/ignore/this, /ignore/that, /also/that_other]  # optional, the endpoints to ignore

sink:
  type: "datahub-rest"
  config:
    server: 'http://localhost:8080'
```

The dataset metadata should be defined directly in the Swagger file, section `["example"]`. If this is not true, the following procedures will take place.

## Config details

### Getting dataset metadata from `forced_example`

You can specify the example in the `forced_examples` part of the
configuration file. In the example, when parsing the endpoint `/accounts/groupname/{name}`, 
the plugin will call the URL:

    https://second_serviceboh-vd.ch/accounts/groupname/test

### Automatically recorded examples

The OpenApi specifications set the listing endpoints just before the detailed ones. 
So, in the list of the methods, you will find

    https:///best_servcice.edu-vd.ch/colors

before

    https://best_servcice.edu-vd.ch/colors/{color}

This plugin is set to automatically keep an example of the data given by the first URL,
which with some probability will include an example of attribute needed by the second.

So, if by calling GET to the first URL you get as response:

    {"pantone code": 100,
     "color": "yellow",
     ...}

the `"color": "yellow"`  part will be used to complete the second link, which
will become:

    https://best_servcice.edu-vd.ch/colors/yellow

to get back the metadata.

### Automatic guessing of IDs

If no useful example is found, a second procedure will try to guess the IDs. So if we have:

    https:///best_servcice.edu-vd.ch/colors/{colorID}

the tool will try to put a number one (1) at the parameter place

    https://best_servcice.edu-vd.ch/colors/1