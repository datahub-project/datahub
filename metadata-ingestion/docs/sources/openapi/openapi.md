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

then this plugin has all the information needed to create the dataset in DataHub.

In case there is no example defined, the plugin will try to get the metadata directly from the endpoint, if it is a `GET` method.
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

the `"color": "yellow"`  part will be used to complete the second link, which
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

and the plugin did not find an example in its previous calls,
the tool has no idea about what to substitute for the `{name}` part.

By specifying in the configuration file

```yaml
    forced_examples:  # optionals
      /accounts/groupname/{name}: ['test']
```

the plugin is able to build a correct URL, as follows:

https://test_endpoint.com/accounts/groupname/test
