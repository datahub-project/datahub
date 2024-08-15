
# Goal

This test is configuration driven by json files which contain request/response sequences intended to
detect unexpected regressions between releases.

Files can be executed in parallel but each request within the file is sequential.

## Adding a test

Create a file for a given OpenAPI version which contains a list of request/response pairs in the following
format.

The request json object is translated into the python request arguments and the response object is the
expected status code and optional body.

```json
[
  {
    "request": {
      "urn": "",
      "description": "",
      "method": "",
      "json": {}
    },
    "response": {
      "status_codes": [200],
      "exclude_regex_paths": [],
      "json": {}
    }
  }
]
```