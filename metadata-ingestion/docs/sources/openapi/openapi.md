The dataset metadata should be defined in one of three possible ways:
1. directly in the Swagger file, section `["example"]`
2. directly in the Swagger file, section `["schema"]`
3. if none from above, by calling endpoint

## Capabilities

The plugin read the swagger file where the endopints are defined and searches for the ones which accept
a `GET`, `POST`, `PATCH` or `PUT` calls.

Datasets are created based on output (response) of `GET`, request body of `POST` and `PATCH`, and both request body and response of `PUT` calls.

In case of optional parameter `get_operations_only` being true, only `GET` calls are being processed. 

### Dataset naming scheme
Name of dataset is derived from OpenAPI title, endpoint path and in certain cases HTTP method concatenated with dots. Path to endpoint has all slashes changed to dots as well.

`Open API title` . `endpoint path`(`optional method type`)

#### GET method
Uses no suffix. Example name might be `Example API.users`.

#### POST method
Uses *__post_request* suffix.
Example name might be `Example API.create-user__post_request`.

#### PATCH method
Uses *__patch_request* suffix. Example name might be `Example API.update-user__patch_request`.

#### PUT method
Uses *__put_request* and *__put_response* suffixes. Example name might be `Example API.update-user__put_request` and/or `Example API.update-user__put_response`

### Tags
Datasets can have assigned tags within DataHub. Some of them are assigned automatically.

#### HTTP method tags
Each dataset will have assigned a tag based on HTTP call method.

#### Tags defined in swagger file
Tags defined in swagger file are always assigned to respective dataset. See the following example:

```yaml
/api/v1/external/google:
put:
  tags:
    - account
    - google
  operationId: finishGoogleAccount
  parameters: []
  requestBody:
    content:
      application/json:
        schema:
          $ref: '#/components/schemas/FinishGoogleAccountDTO'
    required: true
  responses:
    "200":
      description: finishGoogleAccount 200 response
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/LoggedAccountDTO'
```

For every selected endpoint defined in the `paths` section,
the tool searches whether the medatada are already defined in there.

### Deriving scheme from embedded example

If in your swagger file there is the `/api/users/` defined as follows:

```yaml
paths:
  /api/users/:
    get:
      tags: [ "Users" ]
      operationID: GetUsers
      description: Retrieve users data
      responses:
        '200':
          description: Return the list of users
          content:
            application/json:
              example:
                {"user": "username", "name": "Full Name", "job": "any", "is_active": True}
```

then dataset structure may be derived from the provided example.

### Deriving scheme from embedded schema

If there is schema definition included, the structure is retrieved from it:

```yaml
paths:
  /api/users/:
    get:
      tags: [ "Users" ]
      operationID: GetUsers
      description: Retrieve users data
      responses:
        '200':
          description: Return the list of users
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Users'
```

with schema defined within the swagger file as in the following example:

```yaml
components:
  schemas:
    Users:
      required:
        - user
        - name
        - job
        - is_active
      type: object
      properties:
        user:
          type: string
        email:
          type: string
        job:
          type: string
        is_active:
          type: boolean
```

As you can see, in this case this plugin has all the information needed to create the dataset in DataHub. Embedded references to substructures are supported as well. If the documented structure contains a single array, items included in the array are directly described. If an array is included at a lower level of the structure, or there are additional attributes at the top level in addition to an array, the respective attribute of type array is displayed but not type of content of this array. This is due to the fact that [version 2 of fieldPath specification](https://datahubproject.io/docs/advanced/field-path-spec-v2/) is not used during the processing as of now. When possible, native data type is populated with names of embedded structures or format of the respective data type.

### Automatically recorded examples

In case there is no example or schema defined, the plugin will try to get the metadata directly from the endpoint.
So, if in your swagger file you have

```yaml
paths:
  /colors/:
    get:
      tags: [ "Colors" ]
      operationID: GetDefinedColors
      description: Retrieve colors
      responses:
        '200':
          description: Return the list of colors
```

the tool will make a `GET` call to `https:///test_endpoint.com/colors`
and parse the response obtained.

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

the `"color": "yellow"`  part will be used to complete the second link, which
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

* 'get' : this requires username/password combination to be present in the url. Note that {username} and {password} are mandatory placeholders. They will be replaced with the true credentials at runtime. Note that username and password will be sent in the request address, so it's unsecure. If your provider allows for the other method, please go for it.
* 'post' : username and password will be inserted in the body of the POST request

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
      tags: [ "Groups" ]
      operationID: GetGroup
      description: Retrieve group data
      responses:
        '200':
          description: Return details about the group
```

and the plugin did not found an example in its previous calls,
so the tool have no idea about what substitute to the `{name}` part.

By specifying in the configuration file

```yaml
    forced_examples:  # optionals
      /accounts/groupname/{name}: ['test']
```

the plugin is able to build a correct URL, as follows:

https://test_endpoint.com/accounts/groupname/test
