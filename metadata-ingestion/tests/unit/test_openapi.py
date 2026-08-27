import json
import logging
import unittest
from typing import Any, Dict, Generator, List, cast
from unittest.mock import MagicMock, patch

import requests
import yaml
from pydantic import SecretStr, ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.extractor.json_schema_util import get_schema_metadata
from datahub.ingestion.source.openapi import (
    _PARSER_LOGGER_NAME,
    APISource,
    OpenApiConfig,
    OpenApiGetTokenConfig,
    _capture_parser_warnings,
    _CollectingLogHandler,
)
from datahub.ingestion.source.openapi_parser import (
    _join_url,
    check_sw_version,
    extract_fields,
    flatten2list,
    get_endpoints,
    get_schema_from_response,
    get_swag_json,
    get_tok,
    get_url_basepath,
    guessing_url_name,
    maybe_theres_simple_id,
    merge_allof_schemas,
    request_call,
    resolve_schema_references,
    try_guessing,
)

# Shared fixtures for patternProperties / allOf merge tests.
_EMPTY_OPENAPI_SW: Dict[str, Any] = {
    "openapi": "3.0.0",
    "components": {"schemas": {}},
}
_ITEM_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
    },
}
_ITEM_SW_DICT: Dict[str, Any] = {
    "openapi": "3.0.0",
    "components": {"schemas": {"Item": _ITEM_SCHEMA}},
}
_ITEM_ID_ONLY_SW: Dict[str, Any] = {
    "openapi": "3.0.0",
    "components": {
        "schemas": {
            "Item": {
                "type": "object",
                "properties": {"id": {"type": "string"}},
            }
        }
    },
}


class TestGetUrlBasepath:
    def test_base_path_v2(self):
        assert get_url_basepath({"swagger": "2.0", "basePath": "/api/v2"}) == "/api/v2"

    def test_servers_v3(self):
        sw_dict = {"openapi": "3.0.0", "servers": [{"url": "/api/v3"}]}
        assert get_url_basepath(sw_dict) == "/api/v3"

    def test_empty_servers_list(self):
        assert get_url_basepath({"openapi": "3.0.0", "servers": []}) == ""

    def test_server_entry_without_url(self):
        sw_dict = {"openapi": "3.0.0", "servers": [{"description": "prod"}]}
        assert get_url_basepath(sw_dict) == ""

    def test_no_base_path_or_servers(self):
        assert get_url_basepath({"openapi": "3.0.0"}) == ""

    def test_malformed_base_path_falls_through_to_servers(self):
        # Regression: the "servers" branch got an isinstance guard + warning
        # fallback for malformed input, but the "basePath" branch two lines
        # above it had none -- a non-string basePath (e.g. null) used to be
        # returned as-is, breaking the plain string concatenation callers do
        # with the result.
        sw_dict = {
            "openapi": "3.0.0",
            "basePath": None,
            "servers": [{"url": "/api/v3"}],
        }
        assert get_url_basepath(sw_dict) == "/api/v3"

    def test_malformed_server_url_returns_empty_string(self):
        sw_dict = {"openapi": "3.0.0", "servers": [{"url": 123}]}
        assert get_url_basepath(sw_dict) == ""


class TestGetEndpoints(unittest.TestCase):
    # https://github.com/OAI/OpenAPI-Specification/blob/main/examples/v2.0/yaml/api-with-examples.yaml
    openapi20 = """
swagger: "2.0"
info:
  title: Simple API overview
  version: v2
paths:
  /:
    get:
      operationId: listVersionsv2
      summary: List API versions
      produces:
      - application/json
      responses:
        "200":
          description: |-
            200 300 response
          examples:
            application/json: |-
              {
                  "versions": [
                      {
                          "status": "CURRENT",
                          "updated": "2011-01-21T11:33:21Z",
                          "id": "v2.0",
                          "links": [
                              {
                                  "href": "http://127.0.0.1:8774/v2/",
                                  "rel": "self"
                              }
                          ]
                      },
                      {
                          "status": "EXPERIMENTAL",
                          "updated": "2013-07-23T11:33:21Z",
                          "id": "v3.0",
                          "links": [
                              {
                                  "href": "http://127.0.0.1:8774/v3/",
                                  "rel": "self"
                              }
                          ]
                      }
                  ]
              }
        "300":
          description: |-
            200 300 response
          examples:
            application/json: |-
              {
                  "versions": [
                      {
                          "status": "CURRENT",
                          "updated": "2011-01-21T11:33:21Z",
                          "id": "v2.0",
                          "links": [
                              {
                                  "href": "http://127.0.0.1:8774/v2/",
                                  "rel": "self"
                              }
                          ]
                      },
                      {
                          "status": "EXPERIMENTAL",
                          "updated": "2013-07-23T11:33:21Z",
                          "id": "v3.0",
                          "links": [
                              {
                                  "href": "http://127.0.0.1:8774/v3/",
                                  "rel": "self"
                              }
                          ]
                      }
                  ]
              }
  /v2:
    get:
      operationId: getVersionDetailsv2
      summary: Show API version details
      produces:
      - application/json
      responses:
        "200":
          description: |-
            200 203 response
          examples:
            application/json: |-
              {
                  "version": {
                      "status": "CURRENT",
                      "updated": "2011-01-21T11:33:21Z",
                      "media-types": [
                          {
                              "base": "application/xml",
                              "type": "application/vnd.openstack.compute+xml;version=2"
                          },
                          {
                              "base": "application/json",
                              "type": "application/vnd.openstack.compute+json;version=2"
                          }
                      ],
                      "id": "v2.0",
                      "links": [
                          {
                              "href": "http://127.0.0.1:8774/v2/",
                              "rel": "self"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/os-compute-devguide-2.pdf",
                              "type": "application/pdf",
                              "rel": "describedby"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                              "type": "application/vnd.sun.wadl+xml",
                              "rel": "describedby"
                          },
                          {
                            "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                            "type": "application/vnd.sun.wadl+xml",
                            "rel": "describedby"
                          }
                      ]
                  }
              }
        "203":
          description: |-
            200 203 response
          examples:
            application/json: |-
              {
                  "version": {
                      "status": "CURRENT",
                      "updated": "2011-01-21T11:33:21Z",
                      "media-types": [
                          {
                              "base": "application/xml",
                              "type": "application/vnd.openstack.compute+xml;version=2"
                          },
                          {
                              "base": "application/json",
                              "type": "application/vnd.openstack.compute+json;version=2"
                          }
                      ],
                      "id": "v2.0",
                      "links": [
                          {
                              "href": "http://23.253.228.211:8774/v2/",
                              "rel": "self"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/os-compute-devguide-2.pdf",
                              "type": "application/pdf",
                              "rel": "describedby"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                              "type": "application/vnd.sun.wadl+xml",
                              "rel": "describedby"
                          }
                      ]
                  }
              }
    post:
      operationId: updateVersionDetailsv2
      summary: Update API version details
      produces:
      - application/json
      responses:
        "200":
          description: |-
            200 203 response
          examples:
            application/json: |-
              {
                  "version": {
                      "status": "CURRENT",
                      "updated": "2011-01-21T11:33:21Z",
                      "media-types": [
                          {
                              "base": "application/xml",
                              "type": "application/vnd.openstack.compute+xml;version=2"
                          },
                          {
                              "base": "application/json",
                              "type": "application/vnd.openstack.compute+json;version=2"
                          }
                      ],
                      "id": "v2.0",
                      "links": [
                          {
                              "href": "http://127.0.0.1:8774/v2/",
                              "rel": "self"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/os-compute-devguide-2.pdf",
                              "type": "application/pdf",
                              "rel": "describedby"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                              "type": "application/vnd.sun.wadl+xml",
                              "rel": "describedby"
                          },
                          {
                            "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                            "type": "application/vnd.sun.wadl+xml",
                            "rel": "describedby"
                          }
                      ]
                  }
              }
  /v2/updateNoExample:
    post:
      operationId: updateVersionDetailsNoExample
      summary: Show API version details no example output
      produces:
      - application/json
      responses:
        "200":
          description: |-
            200 203 response
  /v2/update:
    post:
      operationId: updateVersionDetailsv2
      summary: Show API version details
      produces:
      - application/json
      responses:
        "200":
          description: |-
            200 203 response
          examples:
            application/json: |-
              {
                  "version": {
                      "status": "CURRENT",
                      "updated": "2011-01-21T11:33:21Z",
                      "media-types": [
                          {
                              "base": "application/xml",
                              "type": "application/vnd.openstack.compute+xml;version=2"
                          },
                          {
                              "base": "application/json",
                              "type": "application/vnd.openstack.compute+json;version=2"
                          }
                      ],
                      "id": "v2.0",
                      "links": [
                          {
                              "href": "http://127.0.0.1:8774/v2/",
                              "rel": "self"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/os-compute-devguide-2.pdf",
                              "type": "application/pdf",
                              "rel": "describedby"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                              "type": "application/vnd.sun.wadl+xml",
                              "rel": "describedby"
                          },
                          {
                            "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                            "type": "application/vnd.sun.wadl+xml",
                            "rel": "describedby"
                          }
                      ]
                  }
              }
consumes:
- application/json
    """

    # https://github.com/OAI/OpenAPI-Specification/blob/main/examples/v3.0/api-with-examples.yaml
    openapi30 = """
openapi: "3.0.0"
info:
  title: Simple API overview
  version: 2.0.0
paths:
  /:
    get:
      operationId: listVersionsv2
      summary: List API versions
      responses:
        '200':
          description: |-
            200 response
          content:
            application/json:
              examples:
                foo:
                  value:
                    {
                      "versions": [
                        {
                            "status": "CURRENT",
                            "updated": "2011-01-21T11:33:21Z",
                            "id": "v2.0",
                            "links": [
                                {
                                    "href": "http://127.0.0.1:8774/v2/",
                                    "rel": "self"
                                }
                            ]
                        },
                        {
                            "status": "EXPERIMENTAL",
                            "updated": "2013-07-23T11:33:21Z",
                            "id": "v3.0",
                            "links": [
                                {
                                    "href": "http://127.0.0.1:8774/v3/",
                                    "rel": "self"
                                }
                            ]
                        }
                      ]
                    }
        '300':
          description: |-
            300 response
          content:
            application/json:
              examples:
                foo:
                  value: |
                   {
                    "versions": [
                          {
                            "status": "CURRENT",
                            "updated": "2011-01-21T11:33:21Z",
                            "id": "v2.0",
                            "links": [
                                {
                                    "href": "http://127.0.0.1:8774/v2/",
                                    "rel": "self"
                                }
                            ]
                        },
                        {
                            "status": "EXPERIMENTAL",
                            "updated": "2013-07-23T11:33:21Z",
                            "id": "v3.0",
                            "links": [
                                {
                                    "href": "http://127.0.0.1:8774/v3/",
                                    "rel": "self"
                                }
                            ]
                        }
                    ]
                   }
  /redirect:
    get:
      operationId: redirectSomewhere
      summary: Redirect to a different endpoint
      responses:
        '302':
          description: 302 response
  /v2:
    get:
      operationId: getVersionDetailsv2
      summary: Show API version details
      responses:
        '200':
          description: |-
            200 response
          content:
            application/json:
              examples:
                foo:
                  value:
                    {
                      "version": {
                        "status": "CURRENT",
                        "updated": "2011-01-21T11:33:21Z",
                        "media-types": [
                          {
                              "base": "application/xml",
                              "type": "application/vnd.openstack.compute+xml;version=2"
                          },
                          {
                              "base": "application/json",
                              "type": "application/vnd.openstack.compute+json;version=2"
                          }
                        ],
                        "id": "v2.0",
                        "links": [
                          {
                              "href": "http://127.0.0.1:8774/v2/",
                              "rel": "self"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/os-compute-devguide-2.pdf",
                              "type": "application/pdf",
                              "rel": "describedby"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                              "type": "application/vnd.sun.wadl+xml",
                              "rel": "describedby"
                          },
                          {
                            "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                            "type": "application/vnd.sun.wadl+xml",
                            "rel": "describedby"
                          }
                        ]
                      }
                    }
        '203':
          description: |-
            203 response
          content:
            application/json:
              examples:
                foo:
                  value:
                    {
                      "version": {
                        "status": "CURRENT",
                        "updated": "2011-01-21T11:33:21Z",
                        "media-types": [
                          {
                              "base": "application/xml",
                              "type": "application/vnd.openstack.compute+xml;version=2"
                          },
                          {
                              "base": "application/json",
                              "type": "application/vnd.openstack.compute+json;version=2"
                          }
                        ],
                        "id": "v2.0",
                        "links": [
                          {
                              "href": "http://23.253.228.211:8774/v2/",
                              "rel": "self"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/os-compute-devguide-2.pdf",
                              "type": "application/pdf",
                              "rel": "describedby"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                              "type": "application/vnd.sun.wadl+xml",
                              "rel": "describedby"
                          }
                        ]
                      }
                    }
  /v2/updateNoExample:
    post:
      operationId: updateVersionDetailsNoExample
      summary: Update API version details
      responses:
        '200':
          description: |-
            200 response
  /v2/update:
    post:
      operationId: updateVersionDetailsv2
      summary: Update API version details
      responses:
        '200':
          description: |-
            200 response
          content:
            application/json:
              examples:
                foo:
                  value:
                    {
                      "version": {
                        "status": "CURRENT",
                        "updated": "2011-01-21T11:33:21Z",
                        "media-types": [
                          {
                              "base": "application/xml",
                              "type": "application/vnd.openstack.compute+xml;version=2"
                          },
                          {
                              "base": "application/json",
                              "type": "application/vnd.openstack.compute+json;version=2"
                          }
                        ],
                        "id": "v2.0",
                        "links": [
                          {
                              "href": "http://127.0.0.1:8774/v2/",
                              "rel": "self"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/os-compute-devguide-2.pdf",
                              "type": "application/pdf",
                              "rel": "describedby"
                          },
                          {
                              "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                              "type": "application/vnd.sun.wadl+xml",
                              "rel": "describedby"
                          },
                          {
                            "href": "http://docs.openstack.org/api/openstack-compute/2/wadl/os-compute-2.wadl",
                            "type": "application/vnd.sun.wadl+xml",
                            "rel": "describedby"
                          }
                        ]
                      }
                    }
"""

    def test_get_endpoints_openapi30(self) -> None:
        """extracting 'get' type endpoints from swagger 3.0 file"""
        sw_file_raw = yaml.safe_load(self.openapi30)
        url_endpoints = get_endpoints(sw_file_raw)

        self.assertEqual(len(url_endpoints), 4)
        d4k = {"data": "", "tags": "", "description": "", "method": ""}
        self.assertEqual(url_endpoints["/"].keys(), d4k.keys())

        self.assertIn("data", url_endpoints["/v2/update"])
        self.assertNotIn("data", url_endpoints["/v2/updateNoExample"])

    def test_get_endpoints_openapi20(self) -> None:
        """extracting 'get' type endpoints from swagger 2.0 file"""
        sw_file_raw = yaml.safe_load(self.openapi20)
        url_endpoints = get_endpoints(sw_file_raw)

        self.assertEqual(len(url_endpoints), 4)
        d4k = {"data": "", "tags": "", "description": "", "method": ""}
        self.assertEqual(url_endpoints["/"].keys(), d4k.keys())

        self.assertIn("data", url_endpoints["/v2/update"])
        self.assertNotIn("data", url_endpoints["/v2/updateNoExample"])

    def test_get_endpoints_prerelease_openapi_version_does_not_crash(self) -> None:
        # Springdoc / Java generators often emit 3.0.1-rc1 style version strings.
        sw_dict = {
            "openapi": "3.0.1-rc1",
            "paths": {
                "/pets": {
                    "get": {
                        "responses": {"200": {"description": "ok"}},
                    }
                }
            },
        }
        check_sw_version(sw_dict)  # must not raise
        endpoints = get_endpoints(sw_dict)
        self.assertIn("/pets", endpoints)

    def test_get_endpoints_malformed_path_item_does_not_abort_other_endpoints(
        self,
    ) -> None:
        # Regression: a single malformed "paths" entry (e.g. a null value from a
        # broken generator) used to raise AttributeError from p_o.get(method),
        # aborting extraction for every other endpoint in the spec too.
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/broken": None,
                "/pets": {"get": {"responses": {"200": {"description": "ok"}}}},
            },
        }
        endpoints = get_endpoints(sw_dict)
        self.assertIn("/pets", endpoints)
        self.assertNotIn("/broken", endpoints)

    def test_get_endpoints_malformed_method_spec_does_not_abort_other_endpoints(
        self,
    ) -> None:
        # Same regression as above, for a malformed method spec (non-object value)
        # instead of a malformed path item.
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/broken": {"get": "oops"},
                "/pets": {"get": {"responses": {"200": {"description": "ok"}}}},
            },
        }
        endpoints = get_endpoints(sw_dict)
        self.assertIn("/pets", endpoints)
        self.assertNotIn("/broken", endpoints)

    def test_get_endpoints_malformed_responses_does_not_abort_other_endpoints(
        self,
    ) -> None:
        # Regression: the malformed-path/method guards above still let a malformed
        # "responses" value (one key deeper) raise AttributeError from
        # responses.get("200"), aborting the whole spec rather than just /broken.
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/broken": {"get": {"responses": None}},
                "/pets": {"get": {"responses": {"200": {"description": "ok"}}}},
            },
        }
        endpoints = get_endpoints(sw_dict)
        self.assertIn("/pets", endpoints)
        self.assertNotIn("/broken", endpoints)

    def test_get_endpoints_malformed_200_response_does_not_abort_other_endpoints(
        self,
    ) -> None:
        # Same regression as above, for a non-object "200" response value (which
        # would otherwise reach check_for_api_example_data's `"content" in base_res`
        # and crash on a non-dict/non-iterable base_res).
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/broken": {"get": {"responses": {"200": 42}}},
                "/pets": {"get": {"responses": {"200": {"description": "ok"}}}},
            },
        }
        endpoints = get_endpoints(sw_dict)
        self.assertIn("/pets", endpoints)
        self.assertNotIn("/broken", endpoints)

    def test_get_endpoints_malformed_tags_warns_and_falls_back_to_empty_list(
        self,
    ) -> None:
        # Regression: a malformed non-list "tags" (e.g. a bare string) used to
        # be stored as-is and later iterated character-by-character by the
        # caller, silently emitting one bogus single-letter tag per character
        # instead of failing loudly.
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/pets": {
                    "get": {
                        "tags": "not-a-list",
                        "responses": {"200": {"description": "ok"}},
                    }
                },
            },
        }
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as cm:
            endpoints = get_endpoints(sw_dict)
        self.assertEqual(endpoints["/pets"]["tags"], [])
        self.assertTrue(any("malformed 'tags'" in msg for msg in cm.output))

    def test_get_endpoints_malformed_description_warns_and_falls_back_to_empty_string(
        self,
    ) -> None:
        # Regression: a non-string description/summary flowed straight into
        # DatasetPropertiesClass with no validation or warning.
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/pets": {
                    "get": {
                        "description": {"not": "a string"},
                        "responses": {"200": {"description": "ok"}},
                    }
                },
            },
        }
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as cm:
            endpoints = get_endpoints(sw_dict)
        self.assertEqual(endpoints["/pets"]["description"], "")
        self.assertTrue(any("malformed 'description'" in msg for msg in cm.output))

    def test_get_endpoints_malformed_content_does_not_abort_other_endpoints(
        self,
    ) -> None:
        # Regression: check_for_api_example_data indexed base_res["content"] with
        # "application/json" without checking it was a dict first. get_endpoints
        # calls it once, globally, before the per-endpoint try/except in
        # APISource.get_workunits_internal exists -- so this crash used to abort
        # extraction for the whole spec, not just /broken.
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/broken": {
                    "get": {"responses": {"200": {"content": ["application/json"]}}}
                },
                "/pets": {"get": {"responses": {"200": {"description": "ok"}}}},
            },
        }
        endpoints = get_endpoints(sw_dict)
        self.assertIn("/pets", endpoints)
        self.assertIn("/broken", endpoints)

    def test_get_endpoints_malformed_json_content_does_not_abort_other_endpoints(
        self,
    ) -> None:
        # Same regression, one level deeper: base_res["content"]["application/json"]
        # not being a dict used to crash on `"example" in json_content` giving a
        # false-positive substring match followed by a TypeError on indexing.
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/broken": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": "text mentioning example"
                                }
                            }
                        }
                    }
                },
                "/pets": {"get": {"responses": {"200": {"description": "ok"}}}},
            },
        }
        endpoints = get_endpoints(sw_dict)
        self.assertIn("/pets", endpoints)
        self.assertIn("/broken", endpoints)

    def test_get_endpoints_malformed_examples_entry_does_not_abort_other_endpoints(
        self,
    ) -> None:
        # Regression: a non-dict value under "examples" (malformed Example Object)
        # used to crash on examples[name].get("value", {}).
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/broken": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {
                                        "examples": {"ex1": "not-a-dict"}
                                    }
                                }
                            }
                        }
                    }
                },
                "/pets": {"get": {"responses": {"200": {"description": "ok"}}}},
            },
        }
        endpoints = get_endpoints(sw_dict)
        self.assertIn("/pets", endpoints)
        self.assertEqual(endpoints["/broken"]["data"], {"ex1": "not-a-dict"})

    def test_get_endpoints_malformed_v2_examples_does_not_abort_other_endpoints(
        self,
    ) -> None:
        # Regression: OpenAPI v2 "examples" not being a dict used to crash on
        # base_res["examples"]["application/json"].
        sw_dict = {
            "swagger": "2.0",
            "paths": {
                "/broken": {"get": {"responses": {"200": {"examples": "oops"}}}},
                "/pets": {"get": {"responses": {"200": {"description": "ok"}}}},
            },
        }
        endpoints = get_endpoints(sw_dict)
        self.assertIn("/pets", endpoints)
        self.assertIn("/broken", endpoints)

    def test_get_endpoints_malformed_csv_content_does_not_abort_other_endpoints(
        self,
    ) -> None:
        # Regression: check_for_api_example_data indexed
        # res_cont["text/csv"]["schema"] without checking "text/csv"'s value was
        # a dict first, crashing on e.g. a bare string there.
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/broken": {
                    "get": {
                        "responses": {"200": {"content": {"text/csv": "not-a-dict"}}}
                    }
                },
                "/pets": {"get": {"responses": {"200": {"description": "ok"}}}},
            },
        }
        endpoints = get_endpoints(sw_dict)
        self.assertIn("/pets", endpoints)
        self.assertIn("/broken", endpoints)

    def test_resolve_schema_references_null_ref_value_does_not_crash(self):
        # Regression: a non-string $ref value (e.g. an empty "$ref:" line, which
        # parses as None in YAML) crashed on ref_path.startswith(...) instead of
        # degrading like any other unsupported ref format (logged as unresolved,
        # then stripped so jsonref never sees the leftover key).
        resolved = resolve_schema_references({"$ref": None}, _EMPTY_OPENAPI_SW)
        self.assertNotIn("$ref", resolved)

    def test_get_url_basepath_malformed_servers_entry_does_not_crash(self) -> None:
        # Regression: a non-object servers[0] (e.g. a bare string) raised
        # AttributeError from .get("url", "") instead of degrading to "".
        self.assertEqual(get_url_basepath({"servers": ["not-a-dict"]}), "")

    def test_check_sw_version_missing_version_logs_warning(self) -> None:
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as cm:
            check_sw_version({"paths": {}})
        self.assertTrue(
            any("no swagger or openapi version key" in msg for msg in cm.output)
        )

    def test_check_sw_version_strips_prerelease_and_build_suffixes(self) -> None:
        # "3.0.1-rc1" and "3.1.0+SNAPSHOT" must compare on major.minor only,
        # not raise or misparse because of the suffix.
        with self.assertNoLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ):
            check_sw_version({"openapi": "3.0.1-rc1"})
        # INFO, not WARNING: every valid 3.1+ spec hits this, so it's purely
        # informational, not something an operator needs to act on.
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="INFO"
        ) as cm:
            check_sw_version({"openapi": "3.1.0+SNAPSHOT"})
        self.assertTrue(any("not been fully tested" in msg for msg in cm.output))

    def test_check_sw_version_malformed_version_logs_warning(self) -> None:
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as cm:
            check_sw_version({"openapi": "abc.def"})
        self.assertTrue(
            any("Unable to parse OpenAPI/Swagger version" in msg for msg in cm.output)
        )

    def test_check_sw_version_non_string_version_logs_warning(self) -> None:
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as cm:
            check_sw_version({"openapi": 3})
        self.assertTrue(
            any("no swagger or openapi version key" in msg for msg in cm.output)
        )


class TestExplodeDict(unittest.TestCase):
    def test_d1(self):
        #  exploding keys of a dict...
        d = {"a": {"b": 3}, "c": 2, "asdasd": {"ytkhj": 2, "uylkj": 3}}

        exp_l = [
            "a",  # parent field
            "a.b",
            "c",
            "asdasd",  # parent field
            "asdasd.ytkhj",
            "asdasd.uylkj",
        ]

        cal_l = flatten2list(d)
        self.assertEqual(
            sorted(exp_l), sorted(cal_l)
        )  # Sort both lists since order doesn't matter


class TestGuessing(unittest.TestCase):
    extr_data = {"advancedcomputersearches": {"id": 202, "name": "_unmanaged"}}

    def test_name_id(self):
        #  guessing in presence of name fields
        url2complete = "/advancedcomputersearches/name/{name}/id/"
        guessed_url = guessing_url_name(url2complete, self.extr_data)
        should_be = "/advancedcomputersearches/name/_unmanaged/id/"
        self.assertEqual(guessed_url, should_be)

    def test_name_id2(self):
        #  guessing in presence of name fields, other
        url2complete = "/advancedcomputersearches/{name}/id/"
        guessed_url = guessing_url_name(url2complete, self.extr_data)
        should_be = "/advancedcomputersearches/_unmanaged/id/"
        self.assertEqual(guessed_url, should_be)

    def test_only_id(self):
        #  guessing in presence of name and id fields
        url2complete = "/advancedcomputersearches/name/{name}/id/{id}"
        guessed_url = guessing_url_name(url2complete, self.extr_data)
        should_be = "/advancedcomputersearches/name/_unmanaged/id/202"
        self.assertEqual(guessed_url, should_be)

    def test_no_k_f(self):
        #  guessing with no known fields
        url2complete = "/advancedcomputersearches/name//id/"
        guessed_url = guessing_url_name(url2complete, self.extr_data)
        self.assertEqual(guessed_url, url2complete)

    def test_one_id(self):
        # guessing url with simple id
        url2complete = "/advancedcomputersearches/name/{id}/"
        guessed_url = maybe_theres_simple_id(url2complete)
        should_be = "/advancedcomputersearches/name/1/"
        self.assertEqual(guessed_url, should_be)

    def test_mul_ids(self):
        # guessing url with multiple simple ids
        url2complete = "/advancedcomputersearches/name/{id}/asd/{id}/jhg"
        guessed_url = maybe_theres_simple_id(url2complete)
        should_be = "/advancedcomputersearches/name/1/asd/1/jhg"
        self.assertEqual(guessed_url, should_be)

    def test_one_cid(self):
        # guessing url with complex id
        url2complete = "/advancedcomputersearches/name/{asdid}/"
        guessed_url = maybe_theres_simple_id(url2complete)
        should_be = "/advancedcomputersearches/name/1/"
        self.assertEqual(guessed_url, should_be)

    def test_mul_cids(self):
        # guessing url with multiple complex ids
        url2complete = "/advancedcomputersearches/name/{asdid}/asd/{asdid}/jhg"
        guessed_url = maybe_theres_simple_id(url2complete)
        should_be = "/advancedcomputersearches/name/1/asd/1/jhg"
        self.assertEqual(guessed_url, should_be)

    extr_data2 = {"advancedcomputersearches": {"id": 202, "name": "_unmanaged"}}

    def test_no_good_guesses(self):
        url2complete = "/advancedcomputersearches/name/{nasde}/asd/{asd}/jhg"
        guessed_url = try_guessing(url2complete, self.extr_data2)
        self.assertEqual(guessed_url, url2complete)


class TestAPISourceSchemaExtraction(unittest.TestCase):
    """Test schema extraction methods in APISource class."""

    def setUp(self):
        """Set up test fixtures."""
        self.ctx = PipelineContext(run_id="test")
        self.config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
        )
        self.source = APISource(self.config, self.ctx, "OpenApi")

    def test_extract_response_schema_from_endpoint_v2(self):
        """Test extracting schema from Swagger v2 response."""
        sw_dict = {
            "swagger": "2.0",
            "definitions": {
                "Pet": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer", "format": "int64"},
                        "name": {"type": "string"},
                    },
                }
            },
        }

        endpoint_spec = {
            "responses": {
                "200": {
                    "description": "Success",
                    "schema": {"$ref": "#/definitions/Pet"},
                }
            }
        }

        result = self.source.extract_response_schema_from_endpoint(
            endpoint_spec, sw_dict
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["type"], "object")
        self.assertIn("id", result["properties"])
        self.assertIn("name", result["properties"])

    def test_extract_response_schema_from_endpoint_v3(self):
        """Test extracting schema from OpenAPI v3 response."""
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "Pet": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer", "format": "int64"},
                            "name": {"type": "string"},
                        },
                    }
                }
            },
        }

        endpoint_spec = {
            "responses": {
                "200": {
                    "description": "Success",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Pet"}
                        }
                    },
                }
            }
        }

        result = self.source.extract_response_schema_from_endpoint(
            endpoint_spec, sw_dict
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["type"], "object")
        self.assertIn("id", result["properties"])
        self.assertIn("name", result["properties"])

    def test_extract_response_schema_no_200_response(self):
        """Test that None is returned when no 200 response exists."""
        endpoint_spec = {
            "responses": {
                "404": {"description": "Not Found"},
                "500": {"description": "Server Error"},
            }
        }

        result = self.source.extract_response_schema_from_endpoint(endpoint_spec, {})

        self.assertIsNone(result)

    def test_extract_response_schema_multiple_content_types(self):
        """Test that application/json is preferred over other content types."""
        sw_dict = {"openapi": "3.0.0"}

        endpoint_spec = {
            "responses": {
                "200": {
                    "description": "Success",
                    "content": {
                        "application/xml": {
                            "schema": {"type": "string", "example": "xml"}
                        },
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {"id": {"type": "integer"}},
                            }
                        },
                        "text/json": {"schema": {"type": "string", "example": "text"}},
                    },
                }
            }
        }

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:
            expected_schema = {
                "type": "object",
                "properties": {"id": {"type": "integer"}},
            }
            mock_get_schema.return_value = expected_schema

            result = self.source.extract_response_schema_from_endpoint(
                endpoint_spec, sw_dict
            )

            self.assertIsNotNone(result)
            # Verify application/json was used (first in priority list)
            self.assertTrue(mock_get_schema.called)
            call_args = mock_get_schema.call_args[0]
            self.assertEqual(
                call_args[0],
                {"type": "object", "properties": {"id": {"type": "integer"}}},
            )

    def test_extract_request_schema_from_endpoint_v3(self):
        """Test extracting from requestBody.content[application/json].schema."""
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "NewPet": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "tag": {"type": "string"},
                        },
                    }
                }
            },
        }

        endpoint_spec = {
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/NewPet"}
                    }
                },
            }
        }

        result = self.source.extract_request_schema_from_endpoint(
            endpoint_spec, sw_dict
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["type"], "object")
        self.assertIn("name", result["properties"])
        self.assertIn("tag", result["properties"])

    def test_extract_request_schema_from_parameters(self):
        """Test extracting schema from parameters (both v2 and v3)."""
        endpoint_spec = {
            "parameters": [
                {
                    "name": "id",
                    "in": "path",
                    "required": True,
                    "schema": {"type": "integer", "format": "int64"},
                },
                {
                    "name": "name",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string"},
                },
            ]
        }

        result = self.source.extract_request_schema_from_endpoint(endpoint_spec, {})

        self.assertIsNotNone(result)
        assert result is not None  # Type assertion for mypy
        self.assertEqual(result["type"], "object")
        self.assertIn("properties", result)
        self.assertIn("id", result["properties"])
        self.assertIn("name", result["properties"])
        self.assertEqual(result["properties"]["id"]["type"], "integer")
        self.assertEqual(result["properties"]["name"]["type"], "string")

    def test_extract_request_schema_no_request_body(self):
        """Test that None is returned when no request body exists."""
        endpoint_spec = {"responses": {"200": {"description": "Success"}}}

        result = self.source.extract_request_schema_from_endpoint(endpoint_spec, {})

        self.assertIsNone(result)

    def test_extract_request_schema_malformed_parameters_warns_and_returns_none(self):
        # Regression: a malformed non-list "parameters" (e.g. a dict instead
        # of a list) was silently iterated as its own keys, producing an
        # empty schema indistinguishable from "no parameters" with no signal
        # that the spec itself is malformed.
        endpoint_spec = {"parameters": {"name": "id"}}
        result = self.source.extract_request_schema_from_endpoint(endpoint_spec, {})
        self.assertIsNone(result)
        self.assertTrue(
            any(
                getattr(w, "title", None) == "Malformed Request Parameters"
                for w in self.source.report.warnings
            )
        )

    def test_extract_request_schema_null_parameters_still_warns(self):
        # Regression: an explicit "parameters: null" is falsy, so a
        # truthiness-first check (`if parameters and not isinstance(...)`)
        # skipped the malformed-input warning entirely for this case.
        endpoint_spec = {"parameters": None}
        result = self.source.extract_request_schema_from_endpoint(endpoint_spec, {})
        self.assertIsNone(result)
        self.assertTrue(
            any(
                getattr(w, "title", None) == "Malformed Request Parameters"
                for w in self.source.report.warnings
            )
        )

    def test_extract_request_schema_malformed_parameters_reaches_report_not_just_log(
        self,
    ):
        # Regression: this warning used to go only through this module's own
        # logger, which _capture_parser_warnings does not wrap (it only
        # bridges openapi_parser's logger) -- so it never reached
        # self.report.warnings, unlike every sibling malformed-input warning.
        endpoint_spec = {"parameters": "not-a-list"}
        self.source.extract_request_schema_from_endpoint(endpoint_spec, {})
        self.assertTrue(
            any(
                getattr(w, "title", None) == "Malformed Request Parameters"
                for w in self.source.report.warnings
            )
        )

    def test_extract_schema_method_priority(self):
        """Test that GET is preferred over POST when both have 200 responses."""
        sw_dict = {
            "swagger": "2.0",
            "paths": {
                "/pets": {
                    "get": {
                        "responses": {
                            "200": {
                                "description": "GET response",
                                "schema": {
                                    "type": "object",
                                    "properties": {"id": {"type": "integer"}},
                                },
                            }
                        }
                    },
                    "post": {
                        "responses": {
                            "200": {
                                "description": "POST response",
                                "schema": {
                                    "type": "object",
                                    "properties": {"name": {"type": "string"}},
                                },
                            }
                        }
                    },
                }
            },
        }

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:
            # Mock to return different schemas for GET vs POST
            def side_effect(schema, sw_dict, **kwargs):
                if "id" in str(schema):
                    return {"type": "object", "properties": {"id": {"type": "integer"}}}
                return {"type": "object", "properties": {"name": {"type": "string"}}}

            mock_get_schema.side_effect = side_effect

            result = self.source.extract_schema_from_all_methods("/pets", sw_dict)

            # Should return GET schema (higher priority)
            self.assertIsNotNone(result)
            assert result is not None  # Type assertion for mypy
            self.assertIn("id", result.get("properties", {}))

    def test_extract_schema_all_methods_order(self):
        """Test that methods are processed in correct priority order (GET, POST, PUT, PATCH)."""
        sw_dict = {
            "swagger": "2.0",
            "paths": {
                "/test": {
                    "patch": {
                        "responses": {
                            "200": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"patch": {"type": "string"}},
                                }
                            }
                        }
                    },
                    "put": {
                        "responses": {
                            "200": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"put": {"type": "string"}},
                                }
                            }
                        }
                    },
                    "post": {
                        "responses": {
                            "200": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"post": {"type": "string"}},
                                }
                            }
                        }
                    },
                    "get": {
                        "responses": {
                            "200": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"get": {"type": "string"}},
                                }
                            }
                        }
                    },
                }
            },
        }

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:

            def side_effect(schema, sw_dict, **kwargs):
                return schema  # Return schema as-is

            mock_get_schema.side_effect = side_effect

            result = self.source.extract_schema_from_all_methods("/test", sw_dict)

            # Should return GET schema (first in priority)
            self.assertIsNotNone(result)
            assert result is not None  # Type assertion for mypy
            self.assertIn("get", result.get("properties", {}))

    def test_extract_schema_fallback_to_lower_priority(self):
        """Test that lower priority methods are used when higher priority methods don't have schemas."""
        sw_dict = {
            "swagger": "2.0",
            "paths": {
                "/test": {
                    "get": {"responses": {"404": {"description": "Not found"}}},
                    "post": {
                        "responses": {
                            "200": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"post_field": {"type": "string"}},
                                }
                            }
                        }
                    },
                }
            },
        }

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:

            def side_effect(schema, sw_dict, **kwargs):
                return schema

            mock_get_schema.side_effect = side_effect

            result = self.source.extract_schema_from_all_methods("/test", sw_dict)

            # Should return POST schema (GET has no 200 response)
            self.assertIsNotNone(result)
            assert result is not None  # Type assertion for mypy
            self.assertIn("post_field", result.get("properties", {}))

    def test_extract_schema_from_openapi_spec_success(self):
        """Test successful schema extraction from spec."""
        sw_dict = {
            "swagger": "2.0",
            "paths": {
                "/pets": {
                    "get": {
                        "responses": {
                            "200": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"id": {"type": "integer"}},
                                }
                            }
                        }
                    }
                }
            },
        }

        with (
            patch(
                "datahub.ingestion.source.openapi.get_schema_from_response"
            ) as mock_get_schema,
            patch.object(
                self.source, "create_schema_metadata_from_schema"
            ) as mock_create_metadata,
        ):
            mock_get_schema.return_value = {
                "type": "object",
                "properties": {"id": {"type": "integer"}},
            }
            mock_create_metadata.return_value = MagicMock()

            result = self.source._extract_schema_from_openapi_spec(
                "/pets", "pets", sw_dict
            )

            self.assertIsNotNone(result)
            mock_create_metadata.assert_called_once()
            self.assertEqual(self.source.schema_extraction_stats.from_openapi_spec, 1)

    def test_extract_schema_from_openapi_spec_no_schema(self):
        """Test that None is returned when no schema found."""
        sw_dict = {
            "swagger": "2.0",
            "paths": {
                "/pets": {"get": {"responses": {"404": {"description": "Not found"}}}}
            },
        }

        result = self.source._extract_schema_from_openapi_spec("/pets", "pets", sw_dict)

        self.assertIsNone(result)

    def test_extract_schema_from_openapi_spec_tracks_stats(self):
        """Test that statistics are properly tracked."""
        sw_dict = {
            "swagger": "2.0",
            "paths": {
                "/pets": {
                    "get": {
                        "responses": {
                            "200": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"id": {"type": "integer"}},
                                }
                            }
                        }
                    }
                }
            },
        }

        initial_count = self.source.schema_extraction_stats.from_openapi_spec

        with (
            patch(
                "datahub.ingestion.source.openapi.get_schema_from_response"
            ) as mock_get_schema,
            patch.object(
                self.source, "create_schema_metadata_from_schema"
            ) as mock_create_metadata,
        ):
            mock_get_schema.return_value = {
                "type": "object",
                "properties": {"id": {"type": "integer"}},
            }
            mock_create_metadata.return_value = MagicMock()

            self.source._extract_schema_from_openapi_spec("/pets", "pets", sw_dict)

            self.assertEqual(
                self.source.schema_extraction_stats.from_openapi_spec, initial_count + 1
            )

    def test_extract_response_schema_handles_exceptions(self):
        """Test that exceptions in response extraction are caught and reported."""
        endpoint_spec = {
            "responses": {
                "200": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "$ref": "#/definitions/Pet"
                            }  # Valid schema that will cause error in processing
                        }
                    }
                }
            }
        }

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:
            # Make get_schema_from_response raise an exception
            mock_get_schema.side_effect = TypeError("Cannot process schema")

            result = self.source.extract_response_schema_from_endpoint(
                endpoint_spec, {}
            )

            # Should return None and report a warning (self.report.warning
            # already logs it -- no separate logger.warning call).
            self.assertIsNone(result)
            self.assertTrue(
                any(
                    getattr(f, "title", None) == "Failed to Extract Response Schema"
                    for f in self.source.report.warnings
                )
            )

    def test_extract_request_schema_handles_exceptions(self):
        """Test that exceptions in request extraction are caught and reported."""
        endpoint_spec = {
            "requestBody": {
                "content": {
                    "application/json": {
                        "schema": {
                            "$ref": "#/components/schemas/NewPet"
                        }  # Valid schema that will cause error in processing
                    }
                }
            }
        }

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:
            # Make get_schema_from_response raise an exception
            mock_get_schema.side_effect = TypeError("Cannot process schema")

            result = self.source.extract_request_schema_from_endpoint(endpoint_spec, {})

            # Should return None and surface a report warning
            self.assertIsNone(result)
            self.assertTrue(len(self.source.report.warnings) > 0)

    def test_resolve_schema_references_recursive(self):
        """Test that nested $ref references are fully resolved in properties, items, etc."""
        sw_dict = {
            "swagger": "2.0",
            "definitions": {
                "Address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"},
                    },
                },
                "Person": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "address": {"$ref": "#/definitions/Address"},
                        "tags": {
                            "type": "array",
                            "items": {"$ref": "#/definitions/Tag"},
                        },
                    },
                },
                "Tag": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                },
            },
        }

        schema = {
            "$ref": "#/definitions/Person",
        }

        resolved = resolve_schema_references(schema, sw_dict)

        # Should resolve all references
        self.assertIsNotNone(resolved)
        self.assertIn("properties", resolved)
        self.assertIn("name", resolved["properties"])
        self.assertIn("address", resolved["properties"])
        # Address should be resolved
        self.assertIn("properties", resolved["properties"]["address"])
        self.assertIn("street", resolved["properties"]["address"]["properties"])
        # Tags array items should be resolved
        self.assertIn("items", resolved["properties"]["tags"])
        self.assertIn("properties", resolved["properties"]["tags"]["items"])

    def test_resolve_schema_references_v2_definitions(self):
        """Test v2 definition references (#/definitions/Pet)."""
        sw_dict = {
            "swagger": "2.0",
            "definitions": {
                "Pet": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer", "format": "int64"},
                        "name": {"type": "string"},
                    },
                }
            },
        }

        schema = {"$ref": "#/definitions/Pet"}

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertIsNotNone(resolved)
        self.assertEqual(resolved["type"], "object")
        self.assertIn("properties", resolved)
        self.assertIn("id", resolved["properties"])
        self.assertIn("name", resolved["properties"])

    def test_resolve_schema_references_v3_components(self):
        """Test v3 component references (#/components/schemas/Pet)."""
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "Pet": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer", "format": "int64"},
                            "name": {"type": "string"},
                        },
                    }
                }
            },
        }

        schema = {"$ref": "#/components/schemas/Pet"}

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertIsNotNone(resolved)
        self.assertEqual(resolved["type"], "object")
        self.assertIn("properties", resolved)
        self.assertIn("id", resolved["properties"])
        self.assertIn("name", resolved["properties"])

    def test_resolve_schema_references_allof_merging(self):
        """Test that allOf schemas are properly merged."""
        sw_dict = {
            "swagger": "2.0",
            "definitions": {
                "NewPet": {
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": {"type": "string"},
                        "tag": {"type": "string"},
                    },
                },
                "Pet": {
                    "type": "object",
                    "allOf": [
                        {"$ref": "#/definitions/NewPet"},
                        {
                            "required": ["id"],
                            "properties": {
                                "id": {"type": "integer", "format": "int64"},
                            },
                        },
                    ],
                },
            },
        }

        schema = {"$ref": "#/definitions/Pet"}

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertIsNotNone(resolved)
        self.assertIn("properties", resolved)
        # Should have merged properties from both allOf entries
        self.assertIn("name", resolved["properties"])
        self.assertIn("tag", resolved["properties"])
        self.assertIn("id", resolved["properties"])
        # Should have merged required fields
        self.assertIn("required", resolved)
        self.assertIn("name", resolved["required"])
        self.assertIn("id", resolved["required"])

    def test_resolve_schema_references_circular(self):
        """Test that circular references are handled by max_depth limit."""
        sw_dict = {
            "swagger": "2.0",
            "definitions": {
                "Pet": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "owner": {"$ref": "#/definitions/Owner"},
                    },
                },
                "Owner": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "pets": {
                            "type": "array",
                            "items": {"$ref": "#/definitions/Pet"},
                        },
                    },
                },
            },
        }

        schema = {"$ref": "#/definitions/Pet"}

        # With max_depth=10, circular references will hit the depth limit
        # and return partially resolved schema instead of RecursionError
        resolved = resolve_schema_references(schema, sw_dict, max_depth=10)

        # Should return partially resolved schema when depth limit is reached
        self.assertIsNotNone(resolved)
        # Should have resolved at least some levels before hitting max_depth
        self.assertIn("properties", resolved)

    def test_resolve_schema_references_max_depth(self):
        """Test that max depth limit prevents infinite recursion."""
        # Create a deeply nested schema
        sw_dict = {
            "swagger": "2.0",
            "definitions": {},
        }

        # Create nested references up to level 20 (exceeds default max_depth of 10)
        definitions: Dict[str, Dict[str, Any]] = {}
        for i in range(0, 20):
            if i < 19:
                definitions[f"Level{i}"] = {
                    "type": "object",
                    "properties": {
                        f"level{i + 1}": {"$ref": f"#/definitions/Level{i + 1}"},
                    },
                }
            else:
                # Last level has no reference
                definitions[f"Level{i}"] = {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"},
                    },
                }
        sw_dict["definitions"] = definitions

        schema = {"$ref": "#/definitions/Level0"}

        # Should handle max depth gracefully without RecursionError
        # With max_depth=10, it should stop before reaching Level10
        resolved = resolve_schema_references(schema, sw_dict, max_depth=10)

        # Should return partially resolved schema when depth limit is reached
        self.assertIsNotNone(resolved)

    def test_resolve_schema_references_pattern_properties_anyof_ref(self):
        sw_dict = _ITEM_SW_DICT
        schema = {
            "type": "object",
            "patternProperties": {
                "^[a-z]+$": {
                    "anyOf": [{"$ref": "#/components/schemas/Item"}],
                }
            },
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertNotIn("$ref", json.dumps(resolved))
        self.assertIn("additionalProperties", resolved)
        self.assertIn("anyOf", resolved["additionalProperties"])
        item = resolved["additionalProperties"]["anyOf"][0]
        self.assertIn("id", item["properties"])
        self.assertIn("name", item["properties"])

        metadata = get_schema_metadata(
            platform="openapi", name="pattern-props", json_schema=resolved
        )
        field_paths = [f.fieldPath for f in metadata.fields]
        self.assertTrue(any(".id" in path for path in field_paths))
        self.assertTrue(any(".name" in path for path in field_paths))

    def test_resolve_schema_references_pattern_properties_keeps_named_properties(self):
        sw_dict = _ITEM_ID_ONLY_SW
        schema = {
            "type": "object",
            "properties": {"fixed": {"type": "string"}},
            "patternProperties": {
                "^[a-z]+$": {"$ref": "#/components/schemas/Item"},
            },
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertNotIn("$ref", json.dumps(resolved))
        self.assertNotIn("additionalProperties", resolved)
        self.assertIn("fixed", resolved["properties"])
        self.assertIn("id", resolved["patternProperties"]["^[a-z]+$"]["properties"])

        metadata = get_schema_metadata(
            platform="openapi", name="named-plus-pattern", json_schema=resolved
        )
        field_paths = [f.fieldPath for f in metadata.fields]
        self.assertTrue(any(".fixed" in path for path in field_paths))

    def test_resolve_schema_references_pattern_properties_after_allof(self):
        sw_dict = _ITEM_ID_ONLY_SW
        schema = {
            "type": "object",
            "patternProperties": {
                "^[a-z]+$": {"$ref": "#/components/schemas/Item"},
            },
            "allOf": [
                {
                    "type": "object",
                    "properties": {"fixed": {"type": "string"}},
                }
            ],
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertNotIn("$ref", json.dumps(resolved))
        self.assertIn("fixed", resolved["properties"])
        self.assertNotIn("additionalProperties", resolved)

        metadata = get_schema_metadata(
            platform="openapi", name="allof-pattern", json_schema=resolved
        )
        field_paths = [f.fieldPath for f in metadata.fields]
        self.assertTrue(any(".fixed" in path for path in field_paths))

    def test_resolve_schema_references_empty_properties_still_promotes(self):
        sw_dict = _ITEM_ID_ONLY_SW
        schema = {
            "type": "object",
            "properties": {},
            "patternProperties": {
                "^[a-z]+$": {"$ref": "#/components/schemas/Item"},
            },
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertNotIn("$ref", json.dumps(resolved))
        self.assertIn("additionalProperties", resolved)
        self.assertIn("id", resolved["additionalProperties"]["properties"])

        metadata = get_schema_metadata(
            platform="openapi", name="empty-props", json_schema=resolved
        )
        self.assertTrue(any(".id" in f.fieldPath for f in metadata.fields))

    def test_resolve_schema_references_pattern_properties_inside_allof(self):
        sw_dict = _ITEM_ID_ONLY_SW
        schema = {
            "allOf": [
                {
                    "type": "object",
                    "patternProperties": {
                        "^[a-z]+$": {"$ref": "#/components/schemas/Item"},
                    },
                }
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertNotIn("$ref", json.dumps(resolved))
        self.assertIn("additionalProperties", resolved)
        self.assertIn("id", resolved["additionalProperties"]["properties"])

    def test_merge_allof_same_pattern_combines_under_allof(self):
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {
                    "type": "object",
                    "patternProperties": {
                        "^[a-z]+$": {
                            "type": "object",
                            "properties": {"a": {"type": "string"}},
                        },
                    },
                },
                {
                    "type": "object",
                    "patternProperties": {
                        "^[a-z]+$": {
                            "type": "object",
                            "properties": {"b": {"type": "string"}},
                        },
                    },
                },
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        pattern_schema = resolved["patternProperties"]["^[a-z]+$"]
        self.assertIn("a", pattern_schema["properties"])
        self.assertIn("b", pattern_schema["properties"])

    def test_merge_allof_same_pattern_three_members_keeps_all_fields(self):
        # 3+ members sharing a pattern must not nest, or earlier schemas get dropped.
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {
                    "type": "object",
                    "patternProperties": {
                        "^[a-z]+$": {"properties": {"a": {"type": "string"}}},
                    },
                },
                {
                    "type": "object",
                    "patternProperties": {
                        "^[a-z]+$": {"properties": {"b": {"type": "string"}}},
                    },
                },
                {
                    "type": "object",
                    "patternProperties": {
                        "^[a-z]+$": {"properties": {"c": {"type": "string"}}},
                    },
                },
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        pattern_schema = resolved["patternProperties"]["^[a-z]+$"]
        for field in ("a", "b", "c"):
            self.assertIn(field, pattern_schema["properties"])

    def test_merge_allof_three_pattern_members_with_ref_resolved(self):
        # 3+ same-pattern members, one a $ref: after merge-then-resolve the flattened
        # allOf members still get their refs resolved (no $ref left, all fields present).
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {"Ref": {"properties": {"c": {"type": "string"}}}}
            },
        }
        schema = {
            "allOf": [
                {
                    "patternProperties": {
                        "^x": {"properties": {"a": {"type": "string"}}}
                    }
                },
                {
                    "patternProperties": {
                        "^x": {"properties": {"b": {"type": "string"}}}
                    }
                },
                {"patternProperties": {"^x": {"$ref": "#/components/schemas/Ref"}}},
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        pattern_schema = resolved["patternProperties"]["^x"]
        self.assertNotIn("$ref", json.dumps(pattern_schema))
        for field in ("a", "b", "c"):
            self.assertIn(field, pattern_schema["properties"])

    def test_merge_allof_same_pattern_preserves_value_constraints(self):
        # Colliding same-pattern value schemas keep scalar constraints after the
        # allOf collapse (merge_allof_schemas carries validation keywords through).
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {"patternProperties": {"^x": {"type": "string", "minLength": 3}}},
                {"patternProperties": {"^x": {"type": "string", "maxLength": 8}}},
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        pattern_schema = resolved["patternProperties"]["^x"]
        self.assertEqual(pattern_schema.get("minLength"), 3)
        self.assertEqual(pattern_schema.get("maxLength"), 8)

    def test_merge_allof_conflicting_bounds_keep_most_restrictive(self):
        # Repeated same-keyword constraints across members collapse to the most
        # restrictive: max of the lower bounds, min of the upper bounds.
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {"patternProperties": {"^x": {"minLength": 2, "maxLength": 9}}},
                {"patternProperties": {"^x": {"minLength": 5, "maxLength": 7}}},
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        pattern_schema = resolved["patternProperties"]["^x"]
        self.assertEqual(pattern_schema.get("minLength"), 5)
        self.assertEqual(pattern_schema.get("maxLength"), 7)
        # Merged integer bounds must stay ints (not coerced to float).
        self.assertIsInstance(pattern_schema.get("minLength"), int)
        self.assertIsInstance(pattern_schema.get("maxLength"), int)

    def test_merge_allof_boolean_exclusive_bounds(self):
        # Equal bounds: exclusivity ORs (True + False → exclusive).
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {
                    "patternProperties": {
                        "^x": {"maximum": 10, "exclusiveMaximum": True}
                    }
                },
                {
                    "patternProperties": {
                        "^x": {"maximum": 10, "exclusiveMaximum": False}
                    }
                },
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        pattern_schema = resolved["patternProperties"]["^x"]
        self.assertEqual(pattern_schema.get("maximum"), 10)
        self.assertIs(pattern_schema.get("exclusiveMaximum"), True)

    def test_merge_allof_exclusive_bound_follows_winning_minimum(self):
        # Weaker exclusive bound must not make a stricter inclusive minimum exclusive.
        # Independent max(minimum)+OR(exclusiveMinimum) would wrongly yield >10.
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {"minimum": 5, "exclusiveMinimum": True},
                {"minimum": 10, "exclusiveMinimum": False},
            ]
        }

        resolved = merge_allof_schemas(schema, sw_dict)

        self.assertEqual(resolved.get("minimum"), 10)
        self.assertIs(resolved.get("exclusiveMinimum"), False)

    def test_merge_allof_orphan_boolean_exclusivity_ignored(self):
        # exclusiveMinimum without a minimum on the same member must not attach to
        # another member's bound (order-dependent otherwise).
        sw_dict = _EMPTY_OPENAPI_SW
        for schema in (
            {
                "allOf": [
                    {"minimum": 10, "exclusiveMinimum": False},
                    {"exclusiveMinimum": True},
                ]
            },
            {
                "allOf": [
                    {"exclusiveMinimum": True},
                    {"minimum": 10, "exclusiveMinimum": False},
                ]
            },
        ):
            resolved = merge_allof_schemas(schema, sw_dict)
            self.assertEqual(resolved.get("minimum"), 10)
            self.assertIs(resolved.get("exclusiveMinimum"), False)

    def test_merge_allof_integer_bounds_preserve_int_type(self):
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "type": "string",
            "minLength": 3,
            "maxLength": 10,
            "allOf": [{"minLength": 5}],
        }

        resolved = merge_allof_schemas(schema, sw_dict)

        self.assertEqual(resolved.get("minLength"), 5)
        self.assertIsInstance(resolved.get("minLength"), int)
        self.assertIsInstance(resolved.get("maxLength"), int)

    def test_merge_allof_pattern_properties_does_not_mutate_shared_dict(self):
        shared: Dict[str, Any] = {"^x": {"type": "string"}}
        schema = {
            "patternProperties": shared,
            "allOf": [{"patternProperties": {"^y": {"type": "integer"}}}],
        }

        resolved = merge_allof_schemas(schema, {})

        self.assertIn("^y", resolved["patternProperties"])
        self.assertEqual(shared, {"^x": {"type": "string"}})
        self.assertIsNot(resolved["patternProperties"], shared)

    def test_resolve_schema_references_does_not_mutate_shared_component_properties(
        self,
    ):
        # Resolving a $ref must not permanently rewrite the component in sw_dict —
        # later endpoints that share the same component would otherwise see leaked
        # nested resolutions from the first caller.
        shared_props: Dict[str, Any] = {
            "id": {"type": "string"},
            "nested": {"$ref": "#/components/schemas/Nested"},
        }
        # Annotate: bare dict + openapi str value otherwise becomes
        # dict[str, Collection[str]] under mypy (str is a Collection[str]).
        sw_dict: Dict[str, Any] = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "Nested": {
                        "type": "object",
                        "properties": {"leaf": {"type": "integer"}},
                    },
                    "Shared": {
                        "type": "object",
                        "properties": shared_props,
                    },
                }
            },
        }
        schema = {"$ref": "#/components/schemas/Shared"}

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertNotIn("$ref", json.dumps(resolved["properties"]["nested"]))
        self.assertEqual(
            resolved["properties"]["nested"]["properties"]["leaf"]["type"], "integer"
        )
        # Original component properties map unchanged (still holds the $ref).
        self.assertEqual(
            sw_dict["components"]["schemas"]["Shared"]["properties"],
            shared_props,
        )
        self.assertEqual(
            shared_props["nested"], {"$ref": "#/components/schemas/Nested"}
        )

    def test_resolve_schema_references_unresolvable_ref_logs_warning(self):
        schema = {"$ref": "#/components/schemas/Missing"}
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as cm:
            resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW)
        # Normalize strips leftover $refs so jsonref never sees them.
        self.assertNotIn("$ref", resolved)
        self.assertTrue(
            any("Unable to resolve schema $ref" in msg for msg in cm.output)
        )
        self.assertTrue(
            any("removed to avoid jsonref failure" in msg for msg in cm.output)
        )

    def test_resolve_schema_references_external_ref_logs_info_not_warning(self):
        # Regression: an external-file $ref (a normal, spec-legal pattern this
        # connector doesn't resolve) was logged identically to a genuinely
        # broken local ref, misleading operators into treating a healthy
        # spec's use of split files as evidence of a malformed spec.
        schema = {"$ref": "external.yaml#/Pet"}
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="INFO"
        ) as cm:
            resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW)
        self.assertNotIn("$ref", resolved)
        # Regression: downgrading the initial "unresolved" notice to INFO
        # wasn't enough -- the later _strip_unresolved_refs postcondition
        # sweep logged its own generic WARNING for any leftover $ref,
        # external or not, so a healthy split-file spec still ended up
        # reported as an "OpenAPI Parsing Warning".
        self.assertFalse(any(record.levelname == "WARNING" for record in cm.records))
        self.assertTrue(
            any("external/unsupported schema $ref" in msg for msg in cm.output)
        )

    def test_resolve_schema_references_mixed_refs_still_warns_on_broken_one(self):
        # A genuinely broken local ref alongside an external one must still
        # surface a WARNING -- downgrading external refs must not mask a
        # real problem elsewhere in the same schema.
        schema = {
            "properties": {
                "a": {"$ref": "external.yaml#/Pet"},
                "b": {"$ref": "#/components/schemas/Missing"},
            }
        }
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as cm:
            resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW)
        self.assertNotIn("$ref", json.dumps(resolved))
        self.assertTrue(
            any("removed to avoid jsonref failure" in msg for msg in cm.output)
        )

    def test_resolve_schema_references_null_components_does_not_crash(self):
        # Regression: `sw_dict.get("components", {}).get("schemas", {})` chained a
        # second .get() before the isinstance guard could run, so an explicit
        # "components: null" (present key, None value) crashed with AttributeError
        # instead of degrading to "ref not found" like the sibling "definitions"
        # (Swagger v2) branch already did.
        schema = {"$ref": "#/components/schemas/Missing"}
        resolved = resolve_schema_references(schema, {"components": None})
        self.assertNotIn("$ref", resolved)

    def test_merge_allof_numeric_exclusive_bounds(self):
        # JSON Schema draft-6+ exclusiveMinimum/Maximum are numeric bounds: keep the
        # most restrictive (max of exclusiveMinimum, min of exclusiveMaximum).
        sw_dict = {**_EMPTY_OPENAPI_SW, "openapi": "3.1.0"}
        schema = {
            "allOf": [
                {"patternProperties": {"^x": {"exclusiveMinimum": 2}}},
                {"patternProperties": {"^x": {"exclusiveMinimum": 5}}},
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        pattern_schema = resolved["patternProperties"]["^x"]
        self.assertEqual(pattern_schema.get("exclusiveMinimum"), 5)

    def test_pattern_properties_promoted_with_additional_properties_false(self):
        # Idiomatic closed-map form: patternProperties + additionalProperties: false.
        # Promotion must still run so json_schema_util can extract the map value type.
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "type": "object",
            "patternProperties": {"^x": {"type": "string"}},
            "additionalProperties": False,
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertEqual(resolved["additionalProperties"], {"type": "string"})

    def test_pattern_properties_not_promoted_over_existing_value_schema(self):
        # An explicit dict additionalProperties value schema is left untouched.
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "type": "object",
            "patternProperties": {"^x": {"type": "string"}},
            "additionalProperties": {"type": "integer"},
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertEqual(resolved["additionalProperties"], {"type": "integer"})

    def test_pattern_properties_not_promoted_when_named_properties_present(self):
        # Regression coverage: a hybrid schema with both a real named field
        # and a catch-all patternProperties must NOT be promoted to a map --
        # json_schema_util treats dict additionalProperties as a map and
        # skips named properties, so promoting here would silently drop "id".
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "patternProperties": {"^x_": {"type": "string"}},
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertIn("id", resolved["properties"])
        self.assertNotIn("additionalProperties", resolved)

    def test_merge_allof_properties_malformed_existing_discarded(self):
        # A malformed non-dict "properties" already on merged_schema (e.g.
        # left by an earlier malformed allOf member) must be discarded
        # rather than crash when a later member contributes a real dict.
        merged = merge_allof_schemas(
            {
                "allOf": [
                    "not-a-schema",
                    {"properties": {"id": {"type": "string"}}},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertEqual(merged["properties"]["id"], {"type": "string"})

    def test_promote_pattern_properties_leaves_conflicting_type_alone(self):
        # An explicit, non-"object" type alongside patternProperties is
        # deliberately left untouched (already-malformed input) rather than
        # overwritten with "object".
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "type": "array",
            "patternProperties": {"^x_": {"type": "string"}},
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertEqual(resolved["type"], "array")

    def test_merge_allof_pattern_properties_malformed_incoming_keeps_existing(self):
        # A malformed (non-bool, non-dict) incoming value must not clobber a
        # real schema already present for the same pattern.
        merged = merge_allof_schemas(
            {
                "allOf": [
                    {"patternProperties": {"^x": {"type": "string"}}},
                    {"patternProperties": {"^x": "not-a-schema"}},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertEqual(merged["patternProperties"]["^x"], {"type": "string"})

    def test_merge_allof_pattern_properties_malformed_existing_takes_incoming(self):
        # A malformed existing value has nothing valid to merge with --
        # whatever the next member contributes (even if also malformed) wins.
        merged = merge_allof_schemas(
            {
                "allOf": [
                    {"patternProperties": {"^x": "not-a-schema"}},
                    {"patternProperties": {"^x": {"type": "string"}}},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertEqual(merged["patternProperties"]["^x"], {"type": "string"})

    def test_merge_allof_identical_oneof_collapses_to_plain_keyword(self):
        # Two allOf members contributing the IDENTICAL oneOf list must
        # collapse to a plain top-level "oneOf", not be needlessly wrapped
        # in a redundant nested allOf.
        one_of = [{"type": "string"}, {"type": "integer"}]
        merged = merge_allof_schemas(
            {"allOf": [{"oneOf": one_of}, {"oneOf": list(one_of)}]},
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertEqual(merged.get("oneOf"), one_of)
        self.assertNotIn("allOf", merged)

    def test_resolve_pattern_properties_malformed_value_warns(self):
        # Regression: every downstream consumer (promotion, normalization,
        # allOf merge) silently no-ops on a non-dict patternProperties with
        # zero signal to the operator that the spec itself is malformed.
        schema = {"type": "object", "patternProperties": ["not-a-dict"]}
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as cm:
            resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW)
        # Left untouched (not crashed, not silently dropped) -- just unresolved.
        self.assertEqual(resolved["patternProperties"], ["not-a-dict"])
        self.assertTrue(
            any("malformed 'patternProperties'" in msg for msg in cm.output)
        )

    def test_merge_allof_mixed_exclusive_bounds_no_error(self):
        # A numeric (draft-6+) exclusive bound in one member and a boolean (draft-4)
        # flag in another must not raise and must keep the numeric bound, regardless
        # of member order.
        sw_dict = {**_EMPTY_OPENAPI_SW, "openapi": "3.1.0"}
        schema = {
            "allOf": [
                {"patternProperties": {"^x": {"exclusiveMinimum": 5}}},
                {"patternProperties": {"^x": {"minimum": 5, "exclusiveMinimum": True}}},
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        pattern_schema = resolved["patternProperties"]["^x"]
        self.assertEqual(pattern_schema.get("exclusiveMinimum"), 5)

    def test_merge_allof_unique_items_or_merge(self):
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {"type": "array", "uniqueItems": False},
                {"type": "array", "uniqueItems": True},
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertTrue(resolved.get("uniqueItems"))

    def test_merge_allof_pattern_and_multiple_of_keep_first(self):
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {"type": "string", "pattern": "^a", "multipleOf": 2},
                {"type": "string", "pattern": "^b", "multipleOf": 3},
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertEqual(resolved.get("pattern"), "^a")
        self.assertEqual(resolved.get("multipleOf"), 2)

    def test_pattern_properties_promoted_with_additional_properties_true(self):
        # additionalProperties: true is the JSON Schema default (≡ absent) and must
        # still promote so map columns are extracted.
        sw_dict = _EMPTY_OPENAPI_SW
        value_schema: Dict[str, Any] = {"type": "string"}
        schema = {
            "type": "object",
            "patternProperties": {"^x": value_schema},
            "additionalProperties": True,
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertEqual(resolved["additionalProperties"], {"type": "string"})
        self.assertIsNot(resolved["additionalProperties"], value_schema)

    def test_merge_allof_additional_properties_deep_merges_across_members(self):
        # Two allOf members each contributing a dict-valued additionalProperties
        # must be deep-merged (matching top-level "properties" semantics), not
        # first-wins/overwritten.
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "properties": {"a": {"type": "string"}},
                    },
                },
                {
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "properties": {"b": {"type": "integer"}},
                    },
                },
            ]
        }

        resolved = merge_allof_schemas(schema, sw_dict)

        additional = resolved["additionalProperties"]
        self.assertEqual(additional["properties"]["a"]["type"], "string")
        self.assertEqual(additional["properties"]["b"]["type"], "integer")

    def test_merge_allof_items_deep_merges_across_members(self):
        # Two allOf members each contributing a distinct "items" schema must be
        # unioned into one item schema via the same merge machinery as
        # top-level "properties", not overwritten by the later member.
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"a": {"type": "string"}},
                    },
                },
                {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"b": {"type": "integer"}},
                    },
                },
            ]
        }

        resolved = merge_allof_schemas(schema, sw_dict)

        items = resolved["items"]
        self.assertEqual(items["properties"]["a"]["type"], "string")
        self.assertEqual(items["properties"]["b"]["type"], "integer")

    def test_merge_allof_property_names_deep_merges_across_members(self):
        # Two plain (non-$ref) allOf members each contributing a distinct
        # propertyNames schema must be combined, not overwritten.
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {"type": "object", "propertyNames": {"minLength": 1}},
                {"type": "object", "propertyNames": {"maxLength": 8}},
            ]
        }

        resolved = merge_allof_schemas(schema, sw_dict)

        names = resolved["propertyNames"]
        self.assertEqual(names.get("minLength"), 1)
        self.assertEqual(names.get("maxLength"), 8)

    def test_merge_allof_mismatched_numeric_bounds_no_error(self):
        # Type-mismatched minLength across allOf members must not TypeError.
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {"type": "string", "minLength": 5},
                {"type": "string", "minLength": "5"},
                {"type": "string", "maxLength": None},
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertEqual(resolved.get("minLength"), 5)

    def test_merge_allof_null_pattern_properties_no_error(self):
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "allOf": [
                {"patternProperties": {"^x": {"type": "string"}}},
                {"patternProperties": None},
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)

        self.assertEqual(
            resolved["patternProperties"]["^x"],
            {"type": "string"},
        )

    def test_multi_pattern_properties_collapse_logs_warning(self):
        sw_dict = _EMPTY_OPENAPI_SW
        schema = {
            "type": "object",
            "patternProperties": {
                "^str_": {"type": "string"},
                "^num_": {"type": "integer"},
            },
        }

        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as logs:
            resolved = resolve_schema_references(schema, sw_dict)

        self.assertIn("anyOf", resolved["additionalProperties"])
        self.assertTrue(
            any("Collapsing" in message for message in logs.output),
            logs.output,
        )

    def test_circular_ref_through_pattern_properties(self):
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "Node": {
                        "type": "object",
                        "patternProperties": {
                            "^child_": {"$ref": "#/components/schemas/Node"},
                        },
                    }
                }
            },
        }
        schema = {"$ref": "#/components/schemas/Node"}

        resolved = resolve_schema_references(schema, sw_dict, max_depth=5)

        self.assertIsNotNone(resolved)
        self.assertIn("patternProperties", resolved)

    def test_extract_response_schema_malformed_no_content_type(self):
        """Test handling of response with content but no application/json."""
        endpoint_spec = {
            "responses": {
                "200": {
                    "description": "Success",
                    "content": {
                        "application/xml": {"schema": {"type": "string"}}
                        # No application/json
                    },
                }
            }
        }

        # Should return None since application/json is not available
        result = self.source.extract_response_schema_from_endpoint(endpoint_spec, {})

        # Should try application/xml as fallback
        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:
            mock_get_schema.return_value = {"type": "string"}
            result = self.source.extract_response_schema_from_endpoint(
                endpoint_spec, {}
            )
            # Should still work with fallback content types
            self.assertIsNotNone(result)
            mock_get_schema.assert_called_once()

    def test_extract_response_schema_malformed_missing_reference(self):
        """Test handling of $ref to non-existent schema."""
        sw_dict = {
            "swagger": "2.0",
            "definitions": {
                # Missing "Pet" definition
            },
        }

        endpoint_spec = {
            "responses": {
                "200": {
                    "description": "Success",
                    "schema": {"$ref": "#/definitions/NonExistent"},
                }
            }
        }

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:
            # get_schema_from_response should handle missing references
            mock_get_schema.return_value = None

            result = self.source.extract_response_schema_from_endpoint(
                endpoint_spec, sw_dict
            )

            # Should return None when reference doesn't exist
            self.assertIsNone(result)

    def test_extract_schema_mixed_methods_get_no_schema_post_has_schema(self):
        """Test that POST schema is used when GET has no schema but POST has schema."""
        sw_dict = {
            "swagger": "2.0",
            "paths": {
                "/pets": {
                    "get": {
                        "responses": {
                            "200": {
                                "description": "Success but no schema"
                                # No schema field
                            }
                        }
                    },
                    "post": {
                        "responses": {
                            "200": {
                                "description": "POST response",
                                "schema": {
                                    "type": "object",
                                    "properties": {"name": {"type": "string"}},
                                },
                            }
                        }
                    },
                }
            },
        }

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:

            def side_effect(schema, sw_dict, **kwargs):
                return schema

            mock_get_schema.side_effect = side_effect

            result = self.source.extract_schema_from_all_methods("/pets", sw_dict)

            # Should return POST schema (GET has no schema)
            self.assertIsNotNone(result)
            assert result is not None  # Type assertion for mypy
            self.assertIn("name", result.get("properties", {}))

    def test_extract_schema_mixed_methods_get_has_schema_post_has_schema(self):
        """Test that GET schema is preferred when both GET and POST have schemas."""
        sw_dict = {
            "swagger": "2.0",
            "paths": {
                "/pets": {
                    "get": {
                        "responses": {
                            "200": {
                                "description": "GET response",
                                "schema": {
                                    "type": "object",
                                    "properties": {"id": {"type": "integer"}},
                                },
                            }
                        }
                    },
                    "post": {
                        "responses": {
                            "200": {
                                "description": "POST response",
                                "schema": {
                                    "type": "object",
                                    "properties": {"name": {"type": "string"}},
                                },
                            }
                        }
                    },
                }
            },
        }

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:

            def side_effect(schema, sw_dict, **kwargs):
                return schema

            mock_get_schema.side_effect = side_effect

            result = self.source.extract_schema_from_all_methods("/pets", sw_dict)

            # Should return GET schema (higher priority)
            self.assertIsNotNone(result)
            assert result is not None  # Type assertion for mypy
            self.assertIn("id", result.get("properties", {}))
            self.assertNotIn("name", result.get("properties", {}))

    def test_extract_response_schema_empty_content(self):
        """Test handling of response with empty content object."""
        endpoint_spec = {
            "responses": {
                "200": {
                    "description": "Success",
                    "content": {},  # Empty content
                }
            }
        }

        result = self.source.extract_response_schema_from_endpoint(endpoint_spec, {})

        # Should return None when content is empty
        self.assertIsNone(result)

    def test_extract_request_schema_malformed_missing_reference(self):
        """Test handling of request body with $ref to non-existent schema."""
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    # Missing "NewPet" definition
                }
            },
        }

        endpoint_spec = {
            "requestBody": {
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/NonExistent"}
                    }
                }
            }
        }

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:
            # get_schema_from_response should handle missing references
            mock_get_schema.return_value = None

            result = self.source.extract_request_schema_from_endpoint(
                endpoint_spec, sw_dict
            )

            # Should return None when reference doesn't exist
            self.assertIsNone(result)

    def test_schema_extraction_with_missing_credentials(self):
        """API calls are skipped when credentials are missing for a GET with 200."""
        config_no_creds = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            enable_api_calls_for_schema_extraction=True,
        )
        source_no_creds = APISource(config_no_creds, self.ctx, "OpenApi")

        # 200 response exists but carries no extractable schema, so the fallback
        # path would otherwise attempt a live GET.
        sw_dict = {
            "swagger": "2.0",
            "paths": {
                "/pets": {
                    "get": {
                        "responses": {
                            "200": {
                                "description": "Success",
                            }
                        }
                    }
                }
            },
        }
        endpoint_dets = {"method": "get", "description": "", "tags": []}

        with patch(
            "datahub.ingestion.source.openapi.request_call"
        ) as mock_request_call:
            list(source_no_creds._process_endpoint("/pets", endpoint_dets, sw_dict, {}))

            mock_request_call.assert_not_called()

        warning_titles = [
            getattr(w, "title", None) or getattr(w, "message", "")
            for w in source_no_creds.report.warnings
        ]
        self.assertTrue(
            any(
                title == "No Schema Extracted - Missing Credentials"
                for title in warning_titles
            ),
            warning_titles,
        )

    def test_extract_schema_from_endpoint_data_list_example_does_not_crash(self):
        # Regression: an OpenAPI "example" value is free-form JSON, so an array
        # response commonly provides a list example (e.g.
        # "example": [{"id": 1}, {"id": 2}]). flatten2list only understands a
        # dict of fields and calls .items() unconditionally, so passing the raw
        # list through crashed with AttributeError, aborting this endpoint's
        # entire processing instead of falling through to the next strategy.
        endpoint_dets = {
            "method": "get",
            "description": "",
            "tags": [],
            "data": [{"id": 1}, {"id": 2}],
        }
        result = self.source._extract_schema_from_endpoint_data(
            endpoint_dets, "test.items"
        )
        self.assertIsNotNone(result)
        assert result is not None
        field_paths = [f.fieldPath for f in result.fields]
        self.assertTrue(any("id" in path for path in field_paths), field_paths)

    def test_extract_schema_from_endpoint_data_scalar_example_does_not_crash(self):
        # Same regression as above for a bare scalar/string example.
        endpoint_dets = {
            "method": "get",
            "description": "",
            "tags": [],
            "data": "just a string",
        }
        result = self.source._extract_schema_from_endpoint_data(
            endpoint_dets, "test.items"
        )
        self.assertIsNone(result)

    def test_extract_schema_from_endpoint_data_list_of_scalars_does_not_crash(self):
        # A list example whose first element is not itself a dict (e.g. a list
        # of plain strings/numbers) has nothing struct-like to extract.
        endpoint_dets = {
            "method": "get",
            "description": "",
            "tags": [],
            "data": [1, 2, 3],
        }
        result = self.source._extract_schema_from_endpoint_data(
            endpoint_dets, "test.items"
        )
        self.assertIsNone(result)

    def test_forced_examples_coerce_numeric_path_params(self):
        # Docs use integers (e.g. /pet/{petId}: [1]); config stores strings for URLs.
        config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            forced_examples={"/pet/{petId}": [1]},
        )
        self.assertEqual(config.forced_examples["/pet/{petId}"], ["1"])

    def test_forced_examples_reject_null_path_params(self):
        with self.assertRaises(ValidationError):
            OpenApiConfig(
                name="test_api",
                url="https://api.example.com",
                swagger_file="/openapi.json",
                forced_examples={"/pet/{petId}": [None]},
            )

    def test_get_token_empty_dict_coerces_to_none(self):
        config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            get_token={},
        )
        self.assertIsNone(config.get_token)

    def test_get_token_get_requires_placeholders(self):
        with self.assertRaises(ValueError):
            OpenApiGetTokenConfig(request_type="get", url_complement="/token")
        cfg = OpenApiGetTokenConfig(
            request_type="get",
            url_complement="/token?u={username}&p={password}",
        )
        self.assertEqual(cfg.request_type, "get")

    def test_property_names_ref_resolved_before_schema_metadata(self):
        # Unresolved propertyNames $ref used to break jsonref and yield empty fields.
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "KeyName": {"type": "string", "minLength": 1},
                    "MapBody": {
                        "type": "object",
                        "propertyNames": {"$ref": "#/components/schemas/KeyName"},
                        "additionalProperties": {"type": "string"},
                    },
                }
            },
        }
        schema = {"$ref": "#/components/schemas/MapBody"}
        resolved = resolve_schema_references(schema, sw_dict)
        self.assertNotIn("$ref", json.dumps(resolved.get("propertyNames")))
        self.assertEqual(resolved["propertyNames"].get("type"), "string")

        metadata = self.source.create_schema_metadata_from_schema("map", resolved)
        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertTrue(len(metadata.fields) > 0)

    def test_property_names_ref_preserves_sibling_allof(self):
        # $ref + sibling allOf under propertyNames must keep the sibling constraints.
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "KeyName": {"type": "string", "minLength": 1},
                }
            },
        }
        schema = {
            "type": "object",
            "propertyNames": {
                "$ref": "#/components/schemas/KeyName",
                "allOf": [{"maxLength": 8}],
            },
            "additionalProperties": {"type": "string"},
        }

        resolved = resolve_schema_references(schema, sw_dict)
        names = resolved["propertyNames"]
        self.assertNotIn("$ref", json.dumps(names))
        self.assertEqual(names.get("type"), "string")
        self.assertEqual(names.get("minLength"), 1)
        self.assertEqual(names.get("maxLength"), 8)

    def test_ref_plus_sibling_nested_ref_resolved(self):
        # $ref + sibling properties that themselves contain $refs must fully resolve;
        # leftover component refs break strict get_schema_metadata conversion.
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "Base": {
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                    },
                    "Extra": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                    },
                }
            },
        }
        schema = {
            "$ref": "#/components/schemas/Base",
            "properties": {
                "extra": {"$ref": "#/components/schemas/Extra"},
            },
        }

        resolved = resolve_schema_references(schema, sw_dict)
        self.assertNotIn("$ref", json.dumps(resolved))
        self.assertIn("id", resolved["properties"])
        self.assertIn("name", resolved["properties"]["extra"]["properties"])

        metadata = self.source.create_schema_metadata_from_schema(
            "ref-sibling", resolved
        )
        self.assertIsNotNone(metadata)
        assert metadata is not None
        field_paths = [f.fieldPath for f in metadata.fields]
        self.assertTrue(any(".id" in path for path in field_paths))
        self.assertTrue(any(".name" in path for path in field_paths))

    def test_plain_allof_refs_resolve_nested_component_refs(self):
        # Standard OpenAPI composition: allOf: [{$ref: A}, {$ref: B}] where B
        # itself nests a $ref must fully expand (not leave B's nested pointer).
        sw_dict: Dict[str, Any] = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "A": {
                        "type": "object",
                        "properties": {"a1": {"type": "string"}},
                    },
                    "B": {
                        "type": "object",
                        "properties": {
                            "b1": {"$ref": "#/components/schemas/C"},
                        },
                    },
                    "C": {
                        "type": "object",
                        "properties": {"c1": {"type": "integer"}},
                    },
                }
            },
        }
        schema = {
            "allOf": [
                {"$ref": "#/components/schemas/A"},
                {"$ref": "#/components/schemas/B"},
            ]
        }

        resolved = resolve_schema_references(schema, sw_dict)
        self.assertNotIn("$ref", json.dumps(resolved))
        self.assertIn("a1", resolved["properties"])
        self.assertIn("c1", resolved["properties"]["b1"]["properties"])

        metadata = self.source.create_schema_metadata_from_schema(
            "allof-compose", resolved
        )
        self.assertIsNotNone(metadata)

    def test_ref_sibling_nested_allof_refs_resolved(self):
        # $ref + sibling property whose value is allOf: [{$ref}, {$ref}] with a
        # further nested $ref must fully resolve (same shallow-merge root cause).
        sw_dict: Dict[str, Any] = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "Base": {
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                    },
                    "X": {
                        "type": "object",
                        "properties": {"x1": {"type": "string"}},
                    },
                    "Y": {
                        "type": "object",
                        "properties": {
                            "y1": {"$ref": "#/components/schemas/Z"},
                        },
                    },
                    "Z": {
                        "type": "object",
                        "properties": {"z1": {"type": "boolean"}},
                    },
                }
            },
        }
        schema = {
            "$ref": "#/components/schemas/Base",
            "properties": {
                "combo": {
                    "allOf": [
                        {"$ref": "#/components/schemas/X"},
                        {"$ref": "#/components/schemas/Y"},
                    ]
                }
            },
        }

        resolved = resolve_schema_references(schema, sw_dict)
        self.assertNotIn("$ref", json.dumps(resolved))
        combo = resolved["properties"]["combo"]
        self.assertIn("x1", combo["properties"])
        self.assertIn("z1", combo["properties"]["y1"]["properties"])

        metadata = self.source.create_schema_metadata_from_schema(
            "sibling-allof", resolved
        )
        self.assertIsNotNone(metadata)

    def test_empty_object_ref_target_with_siblings_resolves(self):
        # {} is a valid component (any-JSON placeholder); truthy-checks must not
        # treat it as unresolvable and leave a raw $ref for jsonref to choke on.
        sw_dict: Dict[str, Any] = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "EmptyObject": {},
                }
            },
        }
        schema = {
            "$ref": "#/components/schemas/EmptyObject",
            "properties": {"note": {"type": "string"}},
        }

        resolved = resolve_schema_references(schema, sw_dict)
        self.assertNotIn("$ref", json.dumps(resolved))
        self.assertEqual(resolved["properties"]["note"]["type"], "string")

        metadata = self.source.create_schema_metadata_from_schema("empty-ref", resolved)
        self.assertIsNotNone(metadata)

    def test_extract_schema_from_openapi_spec_conversion_failure_not_counted(self):
        # Failed conversion must not inflate from_openapi_spec (counter lives in
        # _extract_schema_from_openapi_spec, not create_schema_metadata_from_schema).
        before = self.source.schema_extraction_stats.from_openapi_spec
        with patch.object(
            self.source,
            "extract_schema_from_all_methods",
            return_value={"type": "object", "properties": "not-a-schema"},
        ):
            result = self.source._extract_schema_from_openapi_spec(
                "/bad", "bad", {"openapi": "3.0.0"}
            )
        self.assertIsNone(result)
        self.assertEqual(self.source.schema_extraction_stats.from_openapi_spec, before)
        # A warning, not a failure: _process_endpoint still tries example-data/live-API
        # fallback after this, so a hard failure here would be sticky even if a
        # fallback later succeeds for the same endpoint.
        self.assertTrue(
            any(
                getattr(f, "title", None) == "Failed to Create Schema Metadata"
                for f in self.source.report.warnings
            )
        )

    def test_per_endpoint_isolation_continues_after_failure(self):
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/good": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {"id": {"type": "string"}},
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "/bad": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "$ref": "#/components/schemas/Missing"
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            },
            "components": {"schemas": {}},
        }

        with patch.object(OpenApiConfig, "get_swagger", return_value=sw_dict):
            workunits = list(self.source.get_workunits_internal())

        urns = [
            wu.metadata.entityUrn
            for wu in workunits
            if hasattr(wu.metadata, "entityUrn")
        ]
        self.assertTrue(any("good" in (urn or "") for urn in urns))
        self.assertTrue(any("bad" in (urn or "") for urn in urns))
        # /bad may warn on schema conversion but must not abort the run.
        self.assertFalse(
            any(
                getattr(f, "title", None) == "Failed to Process Endpoint"
                for f in self.source.report.failures
            )
        )

    def test_per_endpoint_isolation_records_process_endpoint_exception(self):
        # Force _process_endpoint itself to raise so the outer try/except is exercised.
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/good": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {"id": {"type": "string"}},
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "/boom": {
                    "get": {
                        "responses": {"200": {"description": "ok"}},
                    }
                },
            },
            "components": {"schemas": {}},
        }

        original = self.source._process_endpoint

        def _boom_on_bad(endpoint_k, endpoint_dets, sw, samples):
            if endpoint_k == "/boom":
                raise RuntimeError("forced endpoint failure")
            return original(endpoint_k, endpoint_dets, sw, samples)

        with (
            patch.object(OpenApiConfig, "get_swagger", return_value=sw_dict),
            patch.object(self.source, "_process_endpoint", side_effect=_boom_on_bad),
        ):
            workunits = list(self.source.get_workunits_internal())

        urns = [
            wu.metadata.entityUrn
            for wu in workunits
            if hasattr(wu.metadata, "entityUrn")
        ]
        self.assertTrue(any("good" in (urn or "") for urn in urns))
        self.assertFalse(any("boom" in (urn or "") for urn in urns))
        self.assertTrue(
            any(
                getattr(f, "title", None) == "Failed to Process Endpoint"
                and any("/boom" in c for c in getattr(f, "context", []))
                for f in self.source.report.failures
            )
        )

    def test_get_swagger_get_token_substitutes_credentials(self):
        config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            username="alice",
            password="s3cret",
            get_token={
                "request_type": "get",
                "url_complement": "/token?u={username}&p={password}",
            },
        )
        with (
            patch(
                "datahub.ingestion.source.openapi.get_tok", return_value="fetched-tok"
            ) as mock_tok,
            patch(
                "datahub.ingestion.source.openapi.get_swag_json",
                return_value={"openapi": "3.0.0", "paths": {}},
            ) as mock_swag,
        ):
            result = config.get_swagger()

        self.assertEqual(result["openapi"], "3.0.0")
        mock_tok.assert_called_once()
        self.assertEqual(mock_tok.call_args.kwargs["method"], "get")
        self.assertEqual(
            mock_tok.call_args.kwargs["tok_url"], "/token?u=alice&p=s3cret"
        )
        mock_swag.assert_called_once()
        self.assertEqual(
            mock_swag.call_args.kwargs["token"],
            "fetched-tok",
        )

    def test_get_swagger_post_token_dispatches_to_get_tok(self):
        config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            username="alice",
            password="s3cret",
            get_token={"request_type": "post", "url_complement": "/auth/token"},
        )
        with (
            patch(
                "datahub.ingestion.source.openapi.get_tok", return_value="post-tok"
            ) as mock_tok,
            patch(
                "datahub.ingestion.source.openapi.get_swag_json",
                return_value={"openapi": "3.0.0", "paths": {}},
            ),
        ):
            config.get_swagger()

        mock_tok.assert_called_once()
        self.assertEqual(mock_tok.call_args.kwargs["method"], "post")
        self.assertEqual(mock_tok.call_args.kwargs["tok_url"], "/auth/token")
        self.assertEqual(mock_tok.call_args.kwargs["username"], "alice")
        self.assertEqual(mock_tok.call_args.kwargs["password"], "s3cret")

    def test_get_swagger_bearer_token_formats_authorization_header(self):
        config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            bearer_token="raw-bearer",
        )
        with patch(
            "datahub.ingestion.source.openapi.get_swag_json",
            return_value={"openapi": "3.0.0", "paths": {}},
        ) as mock_swag:
            config.get_swagger()

        self.assertEqual(mock_swag.call_args.kwargs["token"], "Bearer raw-bearer")
        assert config.token is not None
        self.assertEqual(config.token.get_secret_value(), "Bearer raw-bearer")

    def test_get_swagger_plain_token_passed_through(self):
        config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            token="already-set",
        )
        with patch(
            "datahub.ingestion.source.openapi.get_swag_json",
            return_value={"openapi": "3.0.0", "paths": {}},
        ) as mock_swag:
            config.get_swagger()

        self.assertEqual(mock_swag.call_args.kwargs["token"], "already-set")

    def test_get_swagger_basic_auth_branch(self):
        config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            username="alice",
            password="s3cret",
        )
        with patch(
            "datahub.ingestion.source.openapi.get_swag_json",
            return_value={"openapi": "3.0.0", "paths": {}},
        ) as mock_swag:
            config.get_swagger()

        self.assertEqual(mock_swag.call_args.kwargs["username"], "alice")
        self.assertEqual(mock_swag.call_args.kwargs["password"], "s3cret")
        self.assertNotIn("token", mock_swag.call_args.kwargs)

    def test_get_swagger_empty_bearer_token_falls_back_to_basic_auth(self):
        # Regression: the auth-branch condition mixed truthiness (self.token,
        # self.bearer_token in the inner checks) with `is not None` (in the outer
        # guard), so bearer_token=SecretStr("") took the outer branch but matched
        # none of the inner arms, hitting `assert self.get_token is not None` and
        # crashing with a bare AssertionError instead of falling back to basic auth.
        config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            username="alice",
            password="s3cret",
            bearer_token=SecretStr(""),
        )
        with patch(
            "datahub.ingestion.source.openapi.get_swag_json",
            return_value={"openapi": "3.0.0", "paths": {}},
        ) as mock_swag:
            config.get_swagger()

        self.assertEqual(mock_swag.call_args.kwargs["username"], "alice")
        self.assertNotIn("token", mock_swag.call_args.kwargs)

    def test_make_api_request_request_exception_warns_and_returns_none(self):
        self.config.token = SecretStr("tok")
        with patch(
            "datahub.ingestion.source.openapi.request_call",
            side_effect=requests.exceptions.ConnectionError("down"),
        ):
            result = self.source._make_api_request("https://api.example.com/x")

        self.assertIsNone(result)
        self.assertTrue(
            any(
                getattr(w, "title", None) == "Failed to Call OpenAPI Endpoint"
                for w in self.source.report.warnings
            )
        )

    def test_report_bad_responses_maps_known_status_codes(self):
        # A mapped status code must produce its specific title/message, not
        # the generic fallback -- and must not raise (unlike the previous
        # behavior of aborting the run on an unmapped code).
        self.source.report_bad_responses(401, "GET /secure")
        warning = list(self.source.report.warnings)[-1]
        self.assertEqual(warning.title, "Unauthorized to Extract Metadata")
        self.assertIn("Authentication failed", warning.message)

    def test_report_bad_responses_unmapped_status_code_uses_generic_message(self):
        # Regression: the old code raised Exception(...) for any status code
        # not in a hardcoded set, aborting the whole run. An unmapped code
        # (e.g. 418) must now degrade to a generic warning instead.
        self.source.report_bad_responses(418, "GET /teapot")
        warning = list(self.source.report.warnings)[-1]
        self.assertEqual(warning.title, "Failed to Extract Metadata")
        self.assertIn("Unexpected HTTP status", warning.message)

    def test_report_bad_responses_does_not_double_log(self):
        # Regression: the dict-lookup refactor dropped log=False, so every
        # bad HTTP response was logged twice (once by self.report.warning's
        # default log=True, previously suppressed here deliberately).
        # self.report.warning logs through "datahub.ingestion.api.report",
        # not the source module's own logger.
        with self.assertNoLogs("datahub.ingestion.api.report", level="WARNING"):
            self.source.report_bad_responses(401, "GET /secure")

    def test_ignore_endpoints_skips_workunits(self):
        self.config.ignore_endpoints = ["/skip"]
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/keep": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {"id": {"type": "string"}},
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "/skip": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {"x": {"type": "string"}},
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            },
            "components": {"schemas": {}},
        }
        with patch.object(OpenApiConfig, "get_swagger", return_value=sw_dict):
            workunits = list(self.source.get_workunits_internal())

        urns = [
            wu.metadata.entityUrn
            for wu in workunits
            if hasattr(wu.metadata, "entityUrn")
        ]
        self.assertTrue(any("keep" in (urn or "") for urn in urns))
        self.assertFalse(any("skip" in (urn or "") for urn in urns))

    def test_allof_pattern_properties_with_named_properties_keeps_required(
        self,
    ):
        # Critical: promoting patternProperties per allOf member before merge used to
        # set dict additionalProperties and make json_schema_util drop required fields.
        schema = {
            "allOf": [
                {
                    "type": "object",
                    "patternProperties": {"^a_": {"type": "string"}},
                },
                {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "age": {"type": "integer"},
                    },
                    "required": ["name"],
                },
            ]
        }
        resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW)
        self.assertIn("name", resolved["properties"])
        self.assertIn("age", resolved["properties"])
        self.assertEqual(resolved["required"], ["name"])
        self.assertNotIn("additionalProperties", resolved)
        metadata = get_schema_metadata(
            platform="openapi", name="mixed-allof", json_schema=resolved
        )
        field_paths = [f.fieldPath for f in metadata.fields]
        self.assertTrue(any(".name" in path for path in field_paths))
        self.assertTrue(any(".age" in path for path in field_paths))

    def test_allof_disjoint_pattern_properties_collapse_with_warning(self):
        schema = {
            "allOf": [
                {
                    "type": "object",
                    "patternProperties": {"^a_": {"type": "string"}},
                },
                {
                    "type": "object",
                    "patternProperties": {"^b_": {"type": "integer"}},
                },
            ]
        }
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as logs:
            resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW)
        self.assertIn("^a_", resolved["patternProperties"])
        self.assertIn("^b_", resolved["patternProperties"])
        self.assertIn("anyOf", resolved["additionalProperties"])
        self.assertTrue(any("Collapsing" in msg for msg in logs.output))

    def test_allof_ref_pattern_properties_plus_sibling_named_properties(self):
        sw_dict = {
            "openapi": "3.1.0",
            "components": {
                "schemas": {
                    "MapLike": {
                        "type": "object",
                        "patternProperties": {"^x_": {"type": "string"}},
                    }
                }
            },
        }
        schema = {
            "allOf": [
                {"$ref": "#/components/schemas/MapLike"},
                {
                    "type": "object",
                    "properties": {"id": {"type": "string"}},
                    "required": ["id"],
                },
            ]
        }
        resolved = resolve_schema_references(schema, sw_dict)
        self.assertIn("id", resolved["properties"])
        self.assertEqual(resolved["required"], ["id"])
        self.assertNotIn("additionalProperties", resolved)
        metadata = get_schema_metadata(
            platform="openapi", name="ref-plus-props", json_schema=resolved
        )
        self.assertTrue(any(".id" in f.fieldPath for f in metadata.fields))

    def test_allof_pattern_plus_named_nested_in_items(self):
        schema = {
            "type": "array",
            "items": {
                "allOf": [
                    {
                        "type": "object",
                        "patternProperties": {"^m_": {"type": "string"}},
                    },
                    {
                        "type": "object",
                        "properties": {"label": {"type": "string"}},
                    },
                ]
            },
        }
        resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW)
        items = resolved["items"]
        self.assertIn("label", items["properties"])
        self.assertNotIn("additionalProperties", items)

    def test_allof_pattern_plus_named_nested_in_additional_properties(self):
        schema = {
            "type": "object",
            "additionalProperties": {
                "allOf": [
                    {
                        "type": "object",
                        "patternProperties": {"^m_": {"type": "string"}},
                    },
                    {
                        "type": "object",
                        "properties": {"label": {"type": "string"}},
                    },
                ]
            },
        }
        resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW)
        value_schema = resolved["additionalProperties"]
        self.assertIn("label", value_schema["properties"])
        self.assertNotIn("additionalProperties", value_schema)

    def test_allof_same_named_properties_merge_nested_fields(self):
        schema = {
            "allOf": [
                {
                    "properties": {
                        "p": {
                            "type": "object",
                            "properties": {"a": {"type": "string"}},
                        }
                    }
                },
                {
                    "properties": {
                        "p": {
                            "type": "object",
                            "properties": {"b": {"type": "integer"}},
                        }
                    }
                },
            ]
        }
        resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW)
        nested = resolved["properties"]["p"]["properties"]
        self.assertIn("a", nested)
        self.assertIn("b", nested)

    def test_allof_composition_equals_hand_merged_properties(self):
        # Differential: resolve(allOf) should match resolve(hand-merged).
        m1 = {
            "type": "object",
            "properties": {
                "p": {"type": "object", "properties": {"a": {"type": "string"}}}
            },
        }
        m2 = {
            "type": "object",
            "properties": {
                "p": {"type": "object", "properties": {"b": {"type": "integer"}}}
            },
        }
        composed = resolve_schema_references({"allOf": [m1, m2]}, _EMPTY_OPENAPI_SW)
        hand = resolve_schema_references(
            {
                "type": "object",
                "properties": {
                    "p": {
                        "type": "object",
                        "properties": {
                            "a": {"type": "string"},
                            "b": {"type": "integer"},
                        },
                    }
                },
            },
            _EMPTY_OPENAPI_SW,
        )
        self.assertEqual(
            composed["properties"]["p"]["properties"],
            hand["properties"]["p"]["properties"],
        )

    def test_join_url_inserts_slash_between_host_and_path(self):
        self.assertEqual(
            _join_url("https://api.example.com", "openapi.json"),
            "https://api.example.com/openapi.json",
        )
        self.assertEqual(
            _join_url("https://api.example.com/", "/openapi.json"),
            "https://api.example.com/openapi.json",
        )

    def test_request_call_forwards_proxies_on_basic_auth_branch(self):
        # Regression: proxies was only ever forwarded on the bearer-token branch;
        # a proxy-configured OpenApiConfig using username/password auth silently
        # bypassed the configured proxy on every request.
        with patch("requests.get") as mock_get:
            request_call(
                "https://api.example.com",
                username="u",
                password="p",
                proxies={"https": "https://proxy.example.com"},
            )
        self.assertEqual(
            mock_get.call_args.kwargs["proxies"], {"https": "https://proxy.example.com"}
        )

    def test_request_call_forwards_proxies_on_no_auth_branch(self):
        # Same regression as above, for the neither-token-nor-credentials branch.
        with patch("requests.get") as mock_get:
            request_call(
                "https://api.example.com",
                proxies={"https": "https://proxy.example.com"},
            )
        self.assertEqual(
            mock_get.call_args.kwargs["proxies"], {"https": "https://proxy.example.com"}
        )

    def test_get_swag_json_parses_json_response(self):
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200, content=b'{"openapi": "3.0.0"}'
            )
            result = get_swag_json("https://api.example.com")
        self.assertEqual(result, {"openapi": "3.0.0"})

    def test_get_swag_json_falls_back_to_yaml(self):
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200, content=b"openapi: 3.0.0\npaths: {}\n"
            )
            result = get_swag_json("https://api.example.com")
        self.assertEqual(result, {"openapi": "3.0.0", "paths": {}})

    def test_get_swag_json_raises_when_neither_json_nor_yaml(self):
        with patch("requests.get") as mock_get:
            # A tab character is invalid in both JSON and YAML.
            mock_get.return_value = MagicMock(status_code=200, content=b"{\t*bad*")
            with self.assertRaises(ValueError) as ctx:
                get_swag_json("https://api.example.com")
        self.assertIn("as JSON or YAML", str(ctx.exception))

    def test_get_swag_json_raises_clear_error_on_non_utf8_content(self):
        # Regression: json.loads on non-UTF-8 bytes raises UnicodeDecodeError,
        # not json.JSONDecodeError -- that used to skip both the YAML fallback
        # and this function's own clear error message, letting a raw decode
        # error propagate instead.
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200, content=b"\xff\xfe\x00")
            with self.assertRaises(ValueError) as ctx:
                get_swag_json("https://api.example.com")
        self.assertIn("as JSON or YAML", str(ctx.exception))

    def test_get_swag_json_raises_on_non_dict_document(self):
        # Regression: a valid JSON/YAML document that isn't an object (e.g. a
        # bare list) is not a valid OpenAPI/Swagger spec, but used to be
        # returned as-is despite the declared `-> Dict` contract, letting a
        # confusing TypeError/KeyError surface later in get_endpoints instead
        # of a clear error at the point the malformed data was fetched.
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200, content=b"[1, 2, 3]")
            with self.assertRaises(ValueError) as ctx:
                get_swag_json("https://api.example.com")
        self.assertIn("did not parse to a JSON/YAML object", str(ctx.exception))

    def test_schema_resolution_max_depth_capped(self):
        with self.assertRaises(ValidationError):
            OpenApiConfig(
                name="test_api",
                url="https://api.example.com",
                swagger_file="/openapi.json",
                schema_resolution_max_depth=101,
            )
        with self.assertRaises(ValidationError):
            OpenApiConfig(
                name="test_api",
                url="https://api.example.com",
                swagger_file="/openapi.json",
                schema_resolution_max_depth=0,
            )

    def test_ensure_only_one_token_rejects_token_and_get_token(self):
        with self.assertRaises(ValidationError):
            OpenApiConfig(
                name="test_api",
                url="https://api.example.com",
                swagger_file="/openapi.json",
                token="abc",
                get_token={"request_type": "post", "url_complement": "/auth"},
            )

    def test_ensure_only_one_token_rejects_bearer_and_get_token(self):
        with self.assertRaises(ValidationError):
            OpenApiConfig(
                name="test_api",
                url="https://api.example.com",
                swagger_file="/openapi.json",
                bearer_token="abc",
                get_token={"request_type": "post", "url_complement": "/auth"},
            )

    def test_get_workunits_reports_swagger_fetch_failure(self):
        # The real exception must reach the report (not a generic placeholder):
        # operators need to see the actual cause (401, parse error, TLS failure).
        # This is safe because get_tok already sanitizes every exception it raises
        # (see test_get_tok_*_does_not_leak_credentials_in_message) -- nothing
        # reaching this handler can carry a get_token password.
        with patch.object(
            OpenApiConfig, "get_swagger", side_effect=Exception("spec down")
        ):
            workunits = list(self.source.get_workunits_internal())
        self.assertEqual(workunits, [])
        failures = [
            f
            for f in self.source.report.failures
            if getattr(f, "title", None) == "Failed to Fetch OpenAPI Spec"
        ]
        self.assertTrue(failures)
        self.assertTrue(any("spec down" in c for c in failures[0].context))

    def test_get_workunits_reports_malformed_spec_missing_paths(self):
        # Regression: a spec that fetches/parses fine but lacks "paths" used to
        # raise an unhandled KeyError from get_endpoints, crashing the whole run
        # instead of surfacing a report failure like the sibling get_swagger path.
        with patch.object(
            OpenApiConfig, "get_swagger", return_value={"openapi": "3.0.0"}
        ):
            workunits = list(self.source.get_workunits_internal())
        self.assertEqual(workunits, [])
        self.assertTrue(
            any(
                getattr(f, "title", None) == "Failed to Fetch OpenAPI Spec"
                for f in self.source.report.failures
            )
        )

    def test_get_workunits_forwards_parser_warnings_to_report(self):
        # Regression: openapi_parser's schema-walking functions have no
        # SourceReport access, so malformed-input degradation there (e.g. a
        # non-object path item, skipped by get_endpoints with only a bare
        # logger.warning) never reached the ingestion report an operator
        # actually looks at.
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/pets": "not-a-path-item",
                "/ok": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {"schema": {"type": "object"}}
                                }
                            }
                        }
                    }
                },
            },
        }
        with patch.object(OpenApiConfig, "get_swagger", return_value=sw_dict):
            list(self.source.get_workunits_internal())
        parser_warnings = [
            w
            for w in self.source.report.warnings
            if getattr(w, "title", None) == "OpenAPI Parsing Warning"
        ]
        self.assertEqual(len(parser_warnings), 1)
        self.assertIn("/pets", parser_warnings[0].message)

    def test_get_workunits_does_not_flag_healthy_openapi_31_spec_as_warning(self):
        # Regression: check_sw_version's "not fully tested with Swagger
        # version >3.0" notice is purely informational (every valid 3.1+ spec
        # hits it) but was logged at WARNING, so the new parser-warning bridge
        # forwarded it into the report as if the spec were malformed/needed
        # attention, on every single healthy OpenAPI 3.1 ingestion.
        sw_dict = {
            "openapi": "3.1.0",
            "paths": {
                "/ok": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {"schema": {"type": "object"}}
                                }
                            }
                        }
                    }
                }
            },
        }
        with patch.object(OpenApiConfig, "get_swagger", return_value=sw_dict):
            list(self.source.get_workunits_internal())
        self.assertEqual(
            [
                w
                for w in self.source.report.warnings
                if getattr(w, "title", None) == "OpenAPI Parsing Warning"
            ],
            [],
        )

    def test_get_workunits_flushes_parser_warnings_on_early_generator_close(self):
        # Regression: _report_parser_warnings was only called after the
        # endpoint loop finished (plus once more on the early-fetch-failure
        # return), so a consumer that stops draining this generator early
        # (a GeneratorExit at the `yield from`) skipped it entirely,
        # silently dropping every parser warning captured before that point.
        sw_dict = {
            "openapi": "3.0.0",
            "paths": {
                "/pets": "not-a-path-item",
                "/ok": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {"schema": {"type": "object"}}
                                }
                            }
                        }
                    }
                },
            },
        }
        with patch.object(OpenApiConfig, "get_swagger", return_value=sw_dict):
            gen = cast(Generator[Any, None, None], self.source.get_workunits_internal())
            next(gen)  # advance past the malformed-path-item warning
            gen.close()  # simulate a consumer abandoning the generator early
        parser_warnings = [
            w
            for w in self.source.report.warnings
            if getattr(w, "title", None) == "OpenAPI Parsing Warning"
        ]
        self.assertEqual(len(parser_warnings), 1)
        self.assertIn("/pets", parser_warnings[0].message)

    def test_lookup_local_ref_target_resolves_defs(self):
        # OpenAPI 3.1 adopts JSON Schema 2020-12, where "$defs" is the
        # idiomatic top-level local-definitions container; a $ref using it
        # must resolve like #/components/schemas/... does, not be reported
        # as an unresolved reference on an otherwise valid 3.1 spec.
        sw_dict: Dict[str, Any] = {
            "openapi": "3.1.0",
            "$defs": {
                "Pet": {"type": "object", "properties": {"name": {"type": "string"}}}
            },
        }
        resolved = resolve_schema_references({"$ref": "#/$defs/Pet"}, sw_dict)
        self.assertEqual(resolved, sw_dict["$defs"]["Pet"])

    def test_lookup_local_ref_target_decodes_json_pointer_escapes(self):
        # Regression: a $defs key containing "/" or "~" is escaped per RFC
        # 6901 ("~1"/"~0") inside the $ref fragment. Without decoding, the
        # raw escaped token never matches the real key and the reference
        # silently fails to resolve.
        sw_dict: Dict[str, Any] = {
            "openapi": "3.1.0",
            "$defs": {
                "a/b": {"type": "object", "properties": {"name": {"type": "string"}}}
            },
        }
        resolved = resolve_schema_references({"$ref": "#/$defs/a~1b"}, sw_dict)
        self.assertEqual(resolved, sw_dict["$defs"]["a/b"])

    def test_capture_parser_warnings_ignores_records_from_other_threads(self):
        # Regression: _CollectingLogHandler is thread-scoped so concurrent
        # APISource runs sharing the module-level openapi_parser logger don't
        # attribute each other's parser warnings to the wrong report.
        handler = _CollectingLogHandler()
        owning_thread_record = logging.LogRecord(
            name=_PARSER_LOGGER_NAME,
            level=logging.WARNING,
            pathname=__file__,
            lineno=0,
            msg="from owning thread",
            args=None,
            exc_info=None,
        )
        handler.emit(owning_thread_record)
        self.assertEqual(handler.messages, ["from owning thread"])

        other_thread_record = logging.LogRecord(
            name=_PARSER_LOGGER_NAME,
            level=logging.WARNING,
            pathname=__file__,
            lineno=0,
            msg="from other thread",
            args=None,
            exc_info=None,
        )
        with patch(
            "datahub.ingestion.source.openapi.threading.get_ident",
            return_value=handler._owner_thread + 1,
        ):
            handler.emit(other_thread_record)
        self.assertEqual(handler.messages, ["from owning thread"])

    def test_capture_parser_warnings_refcounted_level_restore(self):
        # Regression: two concurrent _capture_parser_warnings callers sharing
        # the single module-level parser logger must not let the first one
        # to exit restore a level while the second is still relying on it
        # being lowered, nor leave the ambient level permanently overridden.
        parser_logger = logging.getLogger(_PARSER_LOGGER_NAME)
        original_level = parser_logger.level
        parser_logger.setLevel(logging.ERROR)
        try:
            with _capture_parser_warnings() as outer_messages:
                with _capture_parser_warnings() as inner_messages:
                    self.assertEqual(parser_logger.getEffectiveLevel(), logging.WARNING)
                    logging.getLogger(_PARSER_LOGGER_NAME).warning("nested warning")
                # Inner capture exited, but the outer one is still active --
                # the level must still be lowered, not restored to ERROR.
                self.assertEqual(parser_logger.getEffectiveLevel(), logging.WARNING)
            # Both captures exited -- the original ambient level is restored.
            self.assertEqual(parser_logger.level, logging.ERROR)
            self.assertIn("nested warning", outer_messages)
            self.assertIn("nested warning", inner_messages)
        finally:
            parser_logger.setLevel(original_level)

    def test_extract_schema_from_simple_endpoint_live_api_success(self):
        self.source.config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            username="u",
            password="p",
            enable_api_calls_for_schema_extraction=True,
        )
        self.source.url_basepath = ""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"id": 1, "name": "Rex"}'
        with patch(
            "datahub.ingestion.source.openapi.request_call",
            return_value=mock_response,
        ) as mock_call:
            result = self.source._extract_schema_from_simple_endpoint(
                "/pets", "pets", {}
            )
        mock_call.assert_called_once()
        self.assertIsNotNone(result)
        assert result is not None
        field_paths = [f.fieldPath for f in result.fields]
        self.assertTrue(any("id" in path for path in field_paths))
        self.assertEqual(self.source.schema_extraction_stats.from_api_calls, 1)

    def test_extract_schema_from_simple_endpoint_zero_fields_returns_none(self):
        # Regression: a zero-field live-API response used to still return a
        # SchemaMetadataClass (with an empty fields list), unlike the sibling
        # spec/example-data extractors, which correctly return None for a
        # zero-field result -- letting the endpoint fall through to being counted
        # as a real extraction and be emitted as an empty-fields schema aspect.
        self.source.config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            username="u",
            password="p",
            enable_api_calls_for_schema_extraction=True,
        )
        self.source.url_basepath = ""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        with patch(
            "datahub.ingestion.source.openapi.request_call",
            return_value=mock_response,
        ):
            result = self.source._extract_schema_from_simple_endpoint(
                "/pets", "pets", {}
            )
        self.assertIsNone(result)
        self.assertEqual(self.source.schema_extraction_stats.from_api_calls, 0)

    def test_extract_schema_from_parameterized_endpoint_guesses_simple_id(self):
        # The try_guessing branch: no forced_examples for this endpoint, and no
        # prior samples to guess a real value from, so a purely-"{id}"-shaped
        # path falls back to "1".
        self.source.config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            username="u",
            password="p",
            enable_api_calls_for_schema_extraction=True,
        )
        self.source.url_basepath = ""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"id": 1, "name": "Rex"}'
        with patch(
            "datahub.ingestion.source.openapi.request_call",
            return_value=mock_response,
        ) as mock_call:
            result = self.source._extract_schema_from_parameterized_endpoint(
                "/pets/{id}", "pets_by_id", {}
            )
        mock_call.assert_called_once_with(
            "https://api.example.com/pets/1",
            username="u",
            password="p",
            proxies=self.source.config.proxies,
            verify_ssl=True,
        )
        self.assertIsNotNone(result)
        assert result is not None
        field_paths = [f.fieldPath for f in result.fields]
        self.assertTrue(any("id" in path for path in field_paths))

    def test_extract_schema_from_parameterized_endpoint_uses_forced_examples(self):
        self.source.config = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            username="u",
            password="p",
            enable_api_calls_for_schema_extraction=True,
            forced_examples={"/pets/{id}": ["42"]},
        )
        self.source.url_basepath = ""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"id": 42, "name": "Rex"}'
        with patch(
            "datahub.ingestion.source.openapi.request_call",
            return_value=mock_response,
        ) as mock_call:
            result = self.source._extract_schema_from_parameterized_endpoint(
                "/pets/{id}", "pets_by_id", {}
            )
        mock_call.assert_called_once_with(
            "https://api.example.com/pets/42",
            username="u",
            password="p",
            proxies=self.source.config.proxies,
            verify_ssl=True,
        )
        self.assertIsNotNone(result)

    def test_extract_fields_object_response(self):
        response = MagicMock()
        response.content = b'{"id": 1, "nested": {"x": true}}'
        fields, sample = extract_fields(response, "pets")
        self.assertIn("id", fields)
        self.assertIn("nested.x", fields)
        self.assertEqual(sample["id"], 1)

    def test_extract_fields_list_of_scalars_degrades_gracefully(self):
        # Regression: a JSON array whose first element is neither a dict nor
        # a string (e.g. a number) used to raise ValueError("unknown
        # format"), escalating a benign, valid response shape into a hard
        # "Failed to Process Endpoint" failure instead of degrading
        # gracefully like every other unparseable shape in this function.
        response = MagicMock()
        response.content = b"[1, 2, 3]"
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ):
            fields, sample = extract_fields(response, "pets")
        self.assertEqual(fields, [])
        self.assertEqual(sample, {})

    def test_get_tok_post_unexpected_shape_raises(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"not_a_token": true}'
        with patch(
            "datahub.ingestion.source.openapi_parser.requests.post",
            return_value=mock_response,
        ):
            with self.assertRaises(ValueError):
                get_tok(
                    url="https://api.example.com",
                    username="u",
                    password="p",
                    tok_url="/auth",
                    method="post",
                )

    def test_get_tok_unrecognised_method_raises(self):
        # Deliberately passes an invalid method to exercise the runtime guard,
        # despite get_tok's method param now being typed Literal["get", "post"].
        with self.assertRaises(ValueError):
            get_tok(
                url="https://api.example.com",
                tok_url="/auth",
                method="put",  # type: ignore[arg-type]
            )

    def test_get_schema_from_response_untyped_properties(self):
        schema = {"properties": {"id": {"type": "string"}}}
        resolved = get_schema_from_response(schema, _EMPTY_OPENAPI_SW)
        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertIn("id", resolved["properties"])

    def test_get_schema_from_response_top_level_oneof(self):
        schema = {
            "oneOf": [
                {"type": "object", "properties": {"a": {"type": "string"}}},
                {"type": "object", "properties": {"b": {"type": "integer"}}},
            ]
        }
        resolved = get_schema_from_response(schema, _EMPTY_OPENAPI_SW)
        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(len(resolved["oneOf"]), 2)
        self.assertIn("a", resolved["oneOf"][0]["properties"])

    def test_get_schema_from_response_top_level_allof(self):
        schema = {
            "allOf": [
                {"properties": {"id": {"type": "string"}}},
                {"properties": {"name": {"type": "string"}}},
            ]
        }
        resolved = get_schema_from_response(schema, _EMPTY_OPENAPI_SW)
        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertIn("id", resolved["properties"])
        self.assertIn("name", resolved["properties"])

    def test_get_schema_from_response_ref_with_sibling_keywords_preserved(self):
        # Regression: get_schema_from_response used to route top-level $ref (and
        # array "items" $ref) through a bare ref lookup that returned only the
        # referenced target, dropping any sibling keywords (OAS 3.1 / JSON Schema
        # draft-2019-09 allow $ref siblings) instead of resolve_schema_references'
        # proper sibling-merging behavior.
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "Foo": {"properties": {"x": {"type": "string"}}},
                }
            },
        }
        response = {
            "$ref": "#/components/schemas/Foo",
            "properties": {"y": {"type": "integer"}},
        }
        resolved = get_schema_from_response(response, sw_dict)
        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertIn("x", resolved["properties"])
        self.assertIn("y", resolved["properties"])

    def test_get_schema_from_response_array_items_ref_with_siblings_preserved(self):
        # Same regression as above, through the array/"items" branch.
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "Foo": {"properties": {"x": {"type": "string"}}},
                }
            },
        }
        response = {
            "type": "array",
            "items": {
                "$ref": "#/components/schemas/Foo",
                "properties": {"y": {"type": "integer"}},
            },
        }
        resolved = get_schema_from_response(response, sw_dict)
        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertIn("x", resolved["properties"])
        self.assertIn("y", resolved["properties"])

    def test_merge_allof_preserves_oneof_and_discriminator(self):
        schema = {
            "allOf": [
                {"properties": {"id": {"type": "string"}}},
                {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {"kind": {"const": "a"}},
                        },
                        {
                            "type": "object",
                            "properties": {"kind": {"const": "b"}},
                        },
                    ],
                    "discriminator": {"propertyName": "kind"},
                    "nullable": True,
                    "deprecated": True,
                },
            ]
        }
        merged = merge_allof_schemas(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        self.assertIn("id", merged["properties"])
        self.assertEqual(len(merged["oneOf"]), 2)
        self.assertEqual(merged["discriminator"]["propertyName"], "kind")
        self.assertTrue(merged["nullable"])
        self.assertTrue(merged["deprecated"])

    def test_merge_allof_decrements_depth_on_nested_items(self):
        # Deeply nested items allOf must terminate via depth budget, not RecursionError.
        schema: Dict[str, Any] = {"type": "array", "items": {"allOf": []}}
        cursor = schema["items"]
        for _ in range(30):
            nxt = {"type": "array", "items": {"allOf": []}}
            cursor["allOf"] = [
                {"type": "object", "properties": {"x": {"type": "string"}}},
                nxt,
            ]
            cursor = nxt["items"]
        cursor["allOf"] = [
            {"type": "object", "properties": {"leaf": {"type": "string"}}}
        ]

        resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW, max_depth=5)
        self.assertIsNotNone(resolved)
        self.assertNotIn("$ref", json.dumps(resolved))

    def test_max_depth_leaves_no_raw_ref_for_jsonref(self):
        # Circular $ref that hits the depth cap must not hand a raw $ref to jsonref.
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "Node": {
                        "type": "object",
                        "properties": {
                            "child": {"$ref": "#/components/schemas/Node"},
                        },
                    }
                }
            },
        }
        resolved = resolve_schema_references(
            {"$ref": "#/components/schemas/Node"}, sw_dict, max_depth=3
        )
        self.assertFalse(
            '"$ref"' in json.dumps(resolved),
            resolved,
        )
        # Must be consumable by get_schema_metadata without crashing.
        metadata = get_schema_metadata(
            platform="openapi", name="circular", json_schema=resolved
        )
        self.assertIsNotNone(metadata)

    def test_merge_allof_non_list_allof_ignored_with_warning(self):
        # Regression: a truthy but non-list "allOf" (e.g. a dict from a generator
        # bug) used to iterate as if each of its keys were a member schema; every
        # one failed the isinstance(resolved_allof, dict) guard and was silently
        # skipped, discarding everything the malformed allOf contained with zero
        # warning and zero indication anything was wrong with the spec.
        schema = {
            "type": "object",
            "properties": {"kept": {"type": "string"}},
            "allOf": {"type": "object", "properties": {"lost": {"type": "string"}}},
        }
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as cm:
            merged = merge_allof_schemas(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        self.assertNotIn("allOf", merged)
        self.assertIn("kept", merged["properties"])
        self.assertTrue(any("not a list" in msg for msg in cm.output))

    def test_merge_allof_non_list_required_ignored(self):
        # Regression: a malformed spec's "required" (e.g. a bare string instead
        # of a list) used to reach `existing_required + new_required` directly,
        # raising TypeError and dropping the whole merge -- unlike the sibling
        # non-list-"allOf" guard, this one had no isinstance check at all.
        schema = {
            "allOf": [
                {"type": "object", "properties": {"a": {"type": "string"}}},
                {"required": "not-a-list"},
                {"required": ["b"]},
            ]
        }
        merged = merge_allof_schemas(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        self.assertEqual(merged["required"], ["b"])

    def test_merge_allof_root_malformed_required_dropped_even_without_valid_member(
        self,
    ):
        # Regression: the previous fix only skipped a malformed *member*
        # "required" -- it never cleaned up a malformed "required" already on
        # the root schema itself when no allOf member ever contributes a
        # valid list to replace it with, so the invalid value survived
        # untouched and schema extraction still raised downstream.
        schema = {
            "required": "not-a-list",
            "allOf": [{"type": "object", "properties": {"a": {"type": "string"}}}],
        }
        merged = merge_allof_schemas(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        self.assertNotIn("required", merged)

    def test_get_schema_from_response_boolean_schema_returns_none(self):
        # Regression: a bare `true`/`false` root schema (valid JSON Schema,
        # matching merge_allof_schemas' own explicit boolean-member handling)
        # crashed with AttributeError instead of degrading to None.
        self.assertIsNone(get_schema_from_response(True, {}))
        self.assertIsNone(get_schema_from_response(False, {}))

    def test_resolve_schema_references_own_oneof_ref_resolved_before_allof_merge(
        self,
    ):
        # Regression: the schema's own top-level oneOf (sibling to allOf) was
        # popped into oneof_anyof_contributions and, on collision with an allOf
        # member's oneOf, relocated into a terminal allOf wrapper that nothing
        # downstream walks back into -- so if that oneOf still contained an
        # unresolved $ref at the time it was popped, _strip_unresolved_refs would
        # delete it outright instead of it ever being resolved.
        sw_dict = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "X": {"type": "object", "properties": {"xf": {"type": "string"}}}
                }
            },
        }
        # 3+ total oneOf contributors (schema's own + two allOf members): with
        # fewer contributors, the OLD code's own $ref-resolution passes elsewhere
        # in the call graph happened to still surface "xf" as a byproduct of
        # exhausting max_depth (not from correctly resolving it), so a plain
        # substring check on the output can't discriminate old vs. new -- both
        # produce output containing "xf". What DOES discriminate: the old
        # ordering hits max_depth and logs "Maximum recursion depth exceeded"
        # (both for schema references and for allOf merging); the fixed
        # ordering resolves cleanly with no such warning at all.
        schema = {
            "oneOf": [{"$ref": "#/components/schemas/X"}],
            "allOf": [
                {"oneOf": [{"type": "integer"}]},
                {"oneOf": [{"type": "boolean"}]},
            ],
        }
        with self.assertNoLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ):
            resolved = resolve_schema_references(schema, sw_dict)
        self.assertNotIn('"$ref"', json.dumps(resolved))
        resolved_str = json.dumps(resolved)
        self.assertIn("xf", resolved_str)

    def test_merge_allof_depth_cap_preserves_allof_members(self):
        # Hitting max_depth inside merge_allof_schemas must not discard the allOf
        # members outright (previously returned {} for a pure-allOf wrapper).
        schema = {"allOf": [{"properties": {"a": {"type": "string"}}}]}
        merged = merge_allof_schemas(schema, _EMPTY_OPENAPI_SW, max_depth=0)
        self.assertIn("allOf", merged)
        self.assertEqual(merged, schema)

    def test_merge_allof_recursive_allof_chain_terminates(self):
        # The actually-vulnerable shape for unbounded merge_allof_schemas recursion:
        # allOf nested directly inside allOf, with no items/$ref to bound it via a
        # different mechanism. Must terminate via the depth budget, not RecursionError.
        schema: Dict[str, Any] = {"properties": {"leaf": {"type": "string"}}}
        for _ in range(2000):
            schema = {"allOf": [schema]}

        merged = merge_allof_schemas(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        self.assertIsNotNone(merged)

    def test_merge_allof_two_members_both_contribute_oneof(self):
        # Two allOf members each contributing an independent oneOf must both survive
        # (first-wins would silently drop the second member's discriminated union).
        schema = {
            "allOf": [
                {"oneOf": [{"properties": {"catDog": {"type": "string"}}}]},
                {"oneOf": [{"properties": {"sizeSmall": {"type": "string"}}}]},
            ]
        }
        merged = merge_allof_schemas(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        merged_str = json.dumps(merged)
        self.assertIn("catDog", merged_str)
        self.assertIn("sizeSmall", merged_str)

    def test_merge_allof_oneof_collision_does_not_lose_sibling_fields(self):
        # Regression: colliding oneOf/anyOf members used to be deferred into
        # merged_schema["allOf"] and then re-merged by the trailing recursive call,
        # which re-detected the identical collision and looped until max_depth was
        # exhausted -- silently discarding every real field extracted so far. Needs
        # 3+ colliding oneOf contributors to actually trigger the old bug: with only
        # 2, the old pairwise defer-and-recurse happened to terminate cleanly.
        schema = {
            "type": "object",
            "allOf": [
                {"type": "object", "properties": {"payload": {"type": "string"}}},
                {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {"inner_field": {"type": "string"}},
                        }
                    ]
                },
                {
                    "oneOf": [
                        {"type": "object", "properties": {"extra": {"type": "string"}}}
                    ]
                },
                {
                    "oneOf": [
                        {"type": "object", "properties": {"third": {"type": "string"}}}
                    ]
                },
            ],
        }
        resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        self.assertIn("payload", resolved.get("properties", {}))
        metadata = get_schema_metadata(
            platform="openapi", name="oneof-collision", json_schema=resolved
        )
        field_paths = [f.fieldPath for f in metadata.fields]
        self.assertTrue(any("payload" in path for path in field_paths), field_paths)
        self.assertTrue(any("inner_field" in path for path in field_paths), field_paths)
        self.assertTrue(any("extra" in path for path in field_paths), field_paths)
        self.assertTrue(any("third" in path for path in field_paths), field_paths)

    def test_merge_allof_anyof_collision_does_not_lose_sibling_fields(self):
        # Same regression as the oneOf test above, for anyOf -- the shared
        # oneof_anyof_contributions code path handles both keys identically, but
        # nothing else in the test file ever collides on anyOf specifically.
        schema = {
            "type": "object",
            "allOf": [
                {"type": "object", "properties": {"payload": {"type": "string"}}},
                {
                    "anyOf": [
                        {"type": "object", "properties": {"a": {"type": "string"}}}
                    ]
                },
                {
                    "anyOf": [
                        {"type": "object", "properties": {"b": {"type": "string"}}}
                    ]
                },
                {
                    "anyOf": [
                        {"type": "object", "properties": {"c": {"type": "string"}}}
                    ]
                },
            ],
        }
        resolved = resolve_schema_references(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        self.assertNotIn("anyOf", resolved)
        self.assertIn("allOf", resolved)
        metadata = get_schema_metadata(
            platform="openapi", name="anyof-collision", json_schema=resolved
        )
        field_paths = [f.fieldPath for f in metadata.fields]
        for field in ("payload", "a", "b", "c"):
            self.assertTrue(any(field in path for path in field_paths), field_paths)

    def test_merge_allof_own_oneof_survives_allof_member_collision(self):
        # Regression: the schema's own top-level oneOf (a sibling of allOf, not
        # one of its members) used to be silently overwritten by the finalize
        # step's bare `merged_schema[key] = ...` when an allOf member also
        # contributed a colliding oneOf. Old code happened to produce the same
        # a/b/c-containing, oneOf-less output via max-depth exhaustion (its
        # depth-cap fallback returns the unmerged member list, which coincidentally
        # satisfies plain substring/key-absence checks) -- assertNoLogs on the
        # depth-exceeded warning is what actually discriminates "correctly merged"
        # from "gave up and dumped the raw input".
        schema = {
            "type": "object",
            "oneOf": [{"required": ["a"]}, {"required": ["b"]}],
            "allOf": [{"oneOf": [{"required": ["c"]}]}],
        }
        with self.assertNoLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ):
            merged = merge_allof_schemas(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        self.assertNotIn("oneOf", merged)
        self.assertIn("allOf", merged)
        merged_str = json.dumps(merged)
        for field in ("a", "b", "c"):
            self.assertIn(field, merged_str)

    def test_merge_allof_empty_oneof_contribution_dropped(self):
        # Regression: an empty oneOf/anyOf ([]) used to survive into the merged
        # schema (whether as a bare top-level key or a deferred allOf member),
        # and get_schema_metadata's metaschema check rejects "oneOf": [] outright
        # -- losing every field on the schema, not just the empty union.
        schema = {
            "type": "object",
            "properties": {"a": {"type": "string"}},
            "allOf": [{"oneOf": []}],
        }
        merged = merge_allof_schemas(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        self.assertNotIn("oneOf", merged)
        self.assertNotIn("allOf", merged)
        metadata = get_schema_metadata(
            platform="openapi", name="empty-oneof", json_schema=merged
        )
        field_paths = [f.fieldPath for f in metadata.fields]
        self.assertTrue(any("a" in path for path in field_paths), field_paths)

    def test_merge_allof_boolean_member_does_not_crash(self):
        # Regression: a bare `True`/`False` allOf member (valid from JSON Schema
        # draft-6+) reached _merge_allof_properties/_merge_allof_map_keywords
        # unconditionally and crashed with AttributeError ('bool' has no '.get').
        merged = merge_allof_schemas(
            {"allOf": [True, {"properties": {"a": {"type": "string"}}}]},
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertIn("a", merged.get("properties", {}))

    def test_merge_allof_boolean_pattern_properties_value_does_not_crash(self):
        # Same regression as above, specifically through the patternProperties
        # collision path (a boolean subschema as one of the colliding values).
        merged = merge_allof_schemas(
            {
                "allOf": [
                    {"patternProperties": {"^x": {"type": "string"}}},
                    {"patternProperties": {"^x": True}},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertEqual(merged["patternProperties"]["^x"], {"type": "string"})

    def test_merge_allof_pattern_properties_recovers_when_first_member_malformed(self):
        # The reverse ordering of the above: a malformed value arrives first
        # and a real schema arrives second -- the real schema must still be
        # used rather than being discarded because the first slot was junk.
        merged = merge_allof_schemas(
            {
                "allOf": [
                    {"patternProperties": {"^x": True}},
                    {"patternProperties": {"^x": {"type": "string"}}},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertEqual(merged["patternProperties"]["^x"], {"type": "string"})

    def test_merge_allof_pattern_properties_false_wins_collision(self):
        # JSON Schema "false" matches nothing -- it's the most restrictive
        # possible value schema, so it must win any collision outright
        # (order-independent), not be silently discarded by whichever side
        # happens to be a dict.
        merged_false_first = merge_allof_schemas(
            {
                "allOf": [
                    {"patternProperties": {"^x": False}},
                    {"patternProperties": {"^x": {"type": "string"}}},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertIs(merged_false_first["patternProperties"]["^x"], False)

        merged_false_second = merge_allof_schemas(
            {
                "allOf": [
                    {"patternProperties": {"^x": {"type": "string"}}},
                    {"patternProperties": {"^x": False}},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertIs(merged_false_second["patternProperties"]["^x"], False)

    def test_merge_allof_pattern_properties_true_is_noop_against_real_schema(self):
        # JSON Schema "true" matches anything -- colliding with it must not
        # clobber a real schema on the other side.
        merged = merge_allof_schemas(
            {
                "allOf": [
                    {"patternProperties": {"^x": {"type": "string"}}},
                    {"patternProperties": {"^x": True}},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertEqual(merged["patternProperties"]["^x"], {"type": "string"})

    def test_merge_allof_additional_properties_false_wins_collision(self):
        # Regression: unlike the sibling patternProperties merge, a
        # bool/dict collision on additionalProperties fell through both
        # isinstance checks and silently did nothing -- "false" (no extra
        # properties allowed) must win over a dict schema from another
        # allOf member, not be silently dropped.
        merged = merge_allof_schemas(
            {
                "allOf": [
                    {"additionalProperties": {"type": "string"}},
                    {"additionalProperties": False},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertIs(merged["additionalProperties"], False)

    def test_merge_allof_additional_properties_true_is_noop_against_real_schema(self):
        # "true" colliding with a real schema must not clobber it.
        merged = merge_allof_schemas(
            {
                "allOf": [
                    {"additionalProperties": {"type": "string"}},
                    {"additionalProperties": True},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertEqual(merged["additionalProperties"], {"type": "string"})

    def test_merge_allof_property_names_false_wins_collision(self):
        # Regression: unlike its two siblings, propertyNames previously had
        # NO false/true handling at all -- a "propertyNames: false" colliding
        # with a dict schema was silently first-wins instead of correctly
        # winning the intersection.
        merged = merge_allof_schemas(
            {
                "allOf": [
                    {"propertyNames": {"minLength": 1}},
                    {"propertyNames": False},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertIs(merged["propertyNames"], False)

    def test_merge_allof_property_names_true_is_noop_against_real_schema(self):
        # "true" colliding with a real schema must not clobber it.
        merged = merge_allof_schemas(
            {
                "allOf": [
                    {"propertyNames": {"minLength": 1}},
                    {"propertyNames": True},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertEqual(merged["propertyNames"], {"minLength": 1})

    def test_merge_allof_property_true_does_not_clobber_real_schema(self):
        # Regression: unlike additionalProperties/patternProperties/
        # propertyNames, a colliding "properties" entry took the bare
        # assignment path and let a later allOf member's bare "true" (a
        # no-op JSON Schema constraint) clobber an earlier member's real
        # schema -- order-dependent, and a surviving boolean here crashes
        # get_schema_metadata (TypeError: argument of type 'bool' is not
        # iterable).
        merged_a = merge_allof_schemas(
            {
                "allOf": [
                    {"properties": {"a": {"type": "string"}}},
                    {"properties": {"a": True}},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertEqual(merged_a["properties"]["a"], {"type": "string"})

        merged_b = merge_allof_schemas(
            {
                "allOf": [
                    {"properties": {"a": True}},
                    {"properties": {"a": {"type": "string"}}},
                ]
            },
            _EMPTY_OPENAPI_SW,
            max_depth=10,
        )
        self.assertEqual(merged_b["properties"]["a"], {"type": "string"})

    def test_normalize_bare_boolean_property_survives_schema_metadata(self):
        # Regression: a lone "properties": {"a": true} (no allOf collision
        # involved) reached get_schema_metadata as a raw bool and crashed it.
        # "true" (matches anything) normalizes to an empty schema.
        resolved = resolve_schema_references(
            {"type": "object", "properties": {"a": True}}, _EMPTY_OPENAPI_SW
        )
        self.assertEqual(resolved["properties"]["a"], {})
        get_schema_metadata(
            platform="openapi",
            name="bool-property",
            json_schema=resolved,
            swallow_exceptions=False,
        )

    def test_normalize_bare_boolean_property_false_is_dropped(self):
        # "false" (matches nothing) can't be represented as a field schema --
        # drop it rather than let it reach get_schema_metadata.
        resolved = resolve_schema_references(
            {"type": "object", "properties": {"a": False, "b": {"type": "string"}}},
            _EMPTY_OPENAPI_SW,
        )
        self.assertNotIn("a", resolved["properties"])
        self.assertIn("b", resolved["properties"])

    def test_normalize_bare_boolean_items_survives_schema_metadata(self):
        # Regression: a lone "items": true (no allOf collision) reached
        # get_schema_metadata as a raw bool and crashed it.
        resolved = resolve_schema_references(
            {"type": "array", "items": True}, _EMPTY_OPENAPI_SW
        )
        self.assertEqual(resolved["items"], {})

    def test_normalize_bare_boolean_items_false_is_dropped(self):
        resolved = resolve_schema_references(
            {"type": "array", "items": False}, _EMPTY_OPENAPI_SW
        )
        self.assertNotIn("items", resolved)

    def test_normalize_bare_boolean_oneof_member_survives_schema_metadata(self):
        # Regression: a bare boolean member of oneOf/anyOf/allOf is the same
        # crash class as properties/items -- get_schema_metadata raises
        # TypeError on a non-dict schema value, destroying the endpoint's
        # entire spec-derived schema for a legal JSON Schema construct.
        resolved = resolve_schema_references(
            {
                "oneOf": [
                    True,
                    {"type": "object", "properties": {"a": {"type": "string"}}},
                ]
            },
            _EMPTY_OPENAPI_SW,
        )
        self.assertEqual(resolved["oneOf"][0], {})
        get_schema_metadata(
            platform="openapi",
            name="bool-oneof",
            json_schema=resolved,
            swallow_exceptions=False,
        )

    def test_normalize_bare_boolean_oneof_all_false_drops_keyword(self):
        # Every member being unrepresentable ("false") leaves nothing to
        # constrain against -- drop the keyword entirely rather than leave
        # a dangling empty list.
        resolved = resolve_schema_references(
            {"type": "object", "oneOf": [False]}, _EMPTY_OPENAPI_SW
        )
        self.assertNotIn("oneOf", resolved)

    def test_normalize_none_property_value_is_dropped_not_just_bare_bool(self):
        # Regression: the original fix keyed on `is True`/`is False`
        # identity, but ANY non-dict subschema (not just a bare bool)
        # crashes get_schema_metadata identically -- e.g. a hand-written
        # "foo:" with no value parses to None in YAML/JSON.
        resolved = resolve_schema_references(
            {
                "type": "object",
                "properties": {"a": None, "b": {"type": "string"}},
            },
            _EMPTY_OPENAPI_SW,
        )
        self.assertNotIn("a", resolved["properties"])
        self.assertIn("b", resolved["properties"])
        get_schema_metadata(
            platform="openapi",
            name="none-property",
            json_schema=resolved,
            swallow_exceptions=False,
        )

    def test_normalize_none_items_value_is_dropped(self):
        resolved = resolve_schema_references(
            {"type": "array", "items": None}, _EMPTY_OPENAPI_SW
        )
        self.assertNotIn("items", resolved)

    def test_merge_allof_root_required_cleaned_even_with_boolean_member(self):
        # Regression: the required-sanitization ran only inside the dict-only
        # branch, so a boolean allOf member (a valid draft-6+ member that
        # `continue`s past that branch) let a malformed root "required"
        # survive untouched.
        schema = {
            "required": "not-a-list",
            "allOf": [True],
        }
        merged = merge_allof_schemas(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        self.assertNotIn("required", merged)

    def test_merge_allof_three_members_colliding_oneof_all_survive(self):
        # 3+ colliding contributors: a naive pairwise defer-and-recurse loses the
        # third member's oneOf (it gets re-added as a bare top-level "oneOf" that
        # collides with the just-created allOf-wrapped pair on the next pass).
        schema = {
            "allOf": [
                {"oneOf": [{"properties": {"a": {"type": "string"}}}]},
                {"oneOf": [{"properties": {"b": {"type": "string"}}}]},
                {"oneOf": [{"properties": {"c": {"type": "string"}}}]},
            ]
        }
        merged = merge_allof_schemas(schema, _EMPTY_OPENAPI_SW, max_depth=10)
        self.assertNotIn("oneOf", merged)
        self.assertIn("allOf", merged)
        merged_str = json.dumps(merged)
        for field in ("a", "b", "c"):
            self.assertIn(field, merged_str)

    def test_merge_allof_pattern_properties_collision_independent_of_nesting(self):
        # Regression: colliding patternProperties/propertyNames used bare
        # _combine_under_allof (never re-resolved), while properties/items/
        # additionalProperties wrapped the same combination in merge_allof_schemas.
        # The same collision must resolve identically whether the two colliding
        # members are direct allOf siblings or nested one level down via allOf.
        flat = {
            "type": "object",
            "allOf": [
                {"type": "object", "patternProperties": {"^x": {"type": "string"}}},
                {"type": "object", "patternProperties": {"^x": {"type": "integer"}}},
            ],
        }
        nested = {
            "type": "object",
            "allOf": [
                {
                    "allOf": [
                        {
                            "type": "object",
                            "patternProperties": {"^x": {"type": "string"}},
                        },
                        {
                            "type": "object",
                            "patternProperties": {"^x": {"type": "integer"}},
                        },
                    ]
                }
            ],
        }
        # merge_allof_schemas directly, not resolve_schema_references: the public
        # entry point also runs _resolve_pattern_properties as a post-pass, which
        # independently re-resolves any raw allOf wrapper left in patternProperties
        # and would mask this bug on both old and new code -- confirmed old code's
        # merge_allof_schemas alone produces a *different* shape per nesting depth
        # (a leftover {"allOf": [...]} wrapper for "flat", but a clean merge for
        # "nested", since the nested member's own inner allOf gets pre-merged by
        # _resolve_schema_refs before the outer loop ever sees it).
        merged_flat = merge_allof_schemas(flat, _EMPTY_OPENAPI_SW)
        merged_nested = merge_allof_schemas(nested, _EMPTY_OPENAPI_SW)
        self.assertNotIn("allOf", merged_flat["patternProperties"]["^x"])
        self.assertNotIn("allOf", merged_nested["patternProperties"]["^x"])
        self.assertEqual(
            merged_flat["patternProperties"], merged_nested["patternProperties"]
        )

    def test_resolve_schema_references_ref_under_not_does_not_crash(self):
        # A $ref nested under "not" isn't visited by the structural normalize walk;
        # the generic strip pass (not an assert) must still remove it gracefully.
        sw_dict = {
            "openapi": "3.0.0",
            "definitions": {"Foo": {"type": "string"}},
        }
        schema = {
            "type": "object",
            "properties": {"value": {"not": {"$ref": "#/definitions/Foo"}}},
        }
        resolved = resolve_schema_references(schema, sw_dict)
        self.assertNotIn('"$ref"', json.dumps(resolved))

    def test_strip_unresolved_refs_does_not_mutate_shared_component(self):
        # Regression: _strip_unresolved_refs used to `del schema["$ref"]` in place.
        # Neither _resolve_schema_refs nor _normalize_map_schemas walk into "not"
        # (or "if"/"then"/"else"/"contains"), so a $ref nested there is still the
        # exact same dict object as the shared sw_dict component -- deleting its
        # key in place would permanently corrupt that component for every other
        # endpoint that resolves the same $ref later in the same run.
        shared_not_clause: Dict[str, Any] = {
            "$ref": "#/components/schemas/Unresolvable"
        }
        sw_dict: Dict[str, Any] = {
            "openapi": "3.0.0",
            "components": {
                "schemas": {
                    "Shared": {
                        "type": "object",
                        "properties": {"value": {"not": shared_not_clause}},
                    }
                    # "Unresolvable" is intentionally absent so the $ref cannot resolve.
                }
            },
        }
        resolve_schema_references({"$ref": "#/components/schemas/Shared"}, sw_dict)
        # The shared component itself must be untouched by the first resolution.
        self.assertIn("$ref", shared_not_clause)
        self.assertIs(
            sw_dict["components"]["schemas"]["Shared"]["properties"]["value"]["not"],
            shared_not_clause,
        )
        # A second endpoint resolving the same shared component must still get a
        # cleanly-stripped result (not a schema someone else's resolution left broken).
        resolved_again = resolve_schema_references(
            {"$ref": "#/components/schemas/Shared"}, sw_dict
        )
        self.assertNotIn('"$ref"', json.dumps(resolved_again))

    def test_promote_pattern_properties_sets_missing_type_to_object(self):
        # Regression: JsonSchemaTranslator._get_type_from_schema only reaches its
        # "map" branch when type == "object"; a typeless patternProperties-only
        # schema (valid and common in JSON Schema) was promoted to a dict
        # additionalProperties but never got type: object, so it still resolved
        # to zero fields.
        sw_dict = _ITEM_ID_ONLY_SW
        schema = {
            "patternProperties": {
                "^[a-z]+$": {"$ref": "#/components/schemas/Item"},
            },
        }
        resolved = resolve_schema_references(schema, sw_dict)
        self.assertEqual(resolved["type"], "object")

        metadata = get_schema_metadata(
            platform="openapi", name="typeless-map", json_schema=resolved
        )
        self.assertTrue(any(".id" in f.fieldPath for f in metadata.fields))

    def test_resolve_schema_references_depth_capped_node_does_not_mutate_shared_component(
        self,
    ):
        # Regression: when max_depth is exhausted, _resolve_schema_refs returns
        # the original sw_dict component unchanged (not a copy). If that
        # component has a promotable patternProperties shape,
        # _promote_pattern_properties_to_additional/_normalize_map_schemas used
        # to mutate it in place, permanently rewriting the live spec for every
        # other endpoint resolved later in the same run.
        sw_dict: Dict[str, Any] = {
            "openapi": "3.0.0",
            "definitions": {
                f"Level{i}": {"$ref": f"#/definitions/Level{i + 1}"} for i in range(9)
            },
        }
        sw_dict["definitions"]["Level9"] = {
            "patternProperties": {"^x_": {"type": "string"}},
        }
        shared_component = sw_dict["definitions"]["Level9"]

        resolved = resolve_schema_references(
            {"$ref": "#/definitions/Level0"}, sw_dict, max_depth=10
        )

        # The resolved result is promoted...
        self.assertIn("additionalProperties", resolved)
        self.assertEqual(resolved["type"], "object")
        # ...but the shared component itself must be untouched.
        self.assertNotIn("additionalProperties", shared_component)
        self.assertNotIn("type", shared_component)

    def test_strip_unresolved_refs_strips_ref_even_as_property_name_or_example_data(
        self,
    ):
        # jsonref (invoked by get_schema_metadata with swallow_exceptions=False)
        # treats *every* dict with a "$ref" key as a JSON Reference to resolve,
        # regardless of whether it's really a schema keyword, a property
        # literally named "$ref", or a "$ref" key inside "example"/"default"
        # data. There's no way to tell jsonref "this one is just data" --
        # leaving such a key in place makes jsonref raise and drop the whole
        # schema's fields, which is far worse than losing this one key. So
        # _strip_unresolved_refs must strip it unconditionally, even here.
        sw_dict: Dict[str, Any] = {"openapi": "3.0.0"}
        schema = {
            "type": "object",
            "properties": {
                "$ref": {"type": "string", "description": "a field named $ref"},
                "config": {
                    "type": "object",
                    "example": {"$ref": "not-a-schema-ref", "other": "value"},
                    "default": {"$ref": "also-not-a-schema-ref"},
                },
            },
        }
        resolved = resolve_schema_references(schema, sw_dict)
        self.assertNotIn('"$ref"', json.dumps(resolved))

    def test_get_schema_from_response_array_with_boolean_items_falls_through(self):
        # Regression: get_schema_from_response returns None for a bare boolean
        # top-level schema, but the array branch passed a boolean "items" value
        # straight to resolve_schema_references, which returned it unchanged
        # (e.g. `True`) -- truthy, so callers mistook it for a resolved schema
        # and stopped trying other methods/fallbacks.
        sw_dict: Dict[str, Any] = {"openapi": "3.0.0"}
        schema = {"type": "array", "items": True}
        self.assertIsNone(get_schema_from_response(schema, sw_dict))

    def test_get_tok_error_does_not_leak_credentials_in_message(self):
        # get_swagger substitutes {username}/{password} into the GET token URL before
        # calling get_tok; its error messages must never echo that URL or the raw
        # response body back into report.failure.
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200, content=b"not json", text="secret=hunter2"
            )
            with self.assertRaises(ValueError) as ctx:
                get_tok(
                    url="https://api.example.com",
                    tok_url="/token?u=alice&p=hunter2",
                    method="get",
                )
        self.assertNotIn("hunter2", str(ctx.exception))
        self.assertNotIn("u=alice", str(ctx.exception))

    def test_get_tok_connection_error_does_not_leak_credentials_in_message(self):
        # Regression: a raw `requests` exception raised while making the GET/POST
        # token request (not just a malformed 200 response) must not propagate its
        # message verbatim -- for method="get" that message can otherwise embed the
        # password-substituted URL, and it ends up in report.failure verbatim.
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.ConnectionError(
                "Failed to resolve host for https://api.example.com/token?u=alice&p=hunter2"
            )
            with self.assertRaises(ValueError) as ctx:
                get_tok(
                    url="https://api.example.com",
                    tok_url="/token?u=alice&p=hunter2",
                    method="get",
                )
        self.assertNotIn("hunter2", str(ctx.exception))
        self.assertNotIn("u=alice", str(ctx.exception))

        with patch("requests.post") as mock_post:
            mock_post.side_effect = requests.exceptions.ConnectionError(
                "Failed to resolve host for https://api.example.com/token"
            )
            with self.assertRaises(ValueError) as ctx:
                get_tok(
                    url="https://api.example.com",
                    username="alice",
                    password="hunter2",
                    tok_url="/token",
                    method="post",
                )
        self.assertNotIn("hunter2", str(ctx.exception))

    def test_get_schema_from_response_empty_shapes_return_none(self):
        # An empty/no-op object-shape schema ("properties": {}, "oneOf": [], etc.)
        # carries no field information; accepting it here would suppress the
        # example-data/live-API fallback for an endpoint whose spec schema is
        # syntactically present but semantically empty.
        empty_shapes: List[Dict[str, Any]] = [
            {"properties": {}},
            {"oneOf": []},
            {"anyOf": []},
            {"allOf": []},
            {"additionalProperties": False},
        ]
        for schema in empty_shapes:
            self.assertIsNone(get_schema_from_response(schema, {}), schema)
        # additionalProperties: true is a meaningful "any properties allowed" map
        # declaration (not a no-op), so it should still be accepted.
        self.assertIsNotNone(
            get_schema_from_response({"additionalProperties": True}, {})
        )

    def test_extract_schema_from_openapi_spec_zero_fields_not_counted_as_success(self):
        # A schema that resolves without error but yields zero fields (e.g. an
        # untyped additionalProperties-only map) must not be counted as a
        # successful extraction or silently reported as one.
        endpoint_spec = {
            "get": {
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {"additionalProperties": {"type": "string"}}
                            }
                        }
                    }
                }
            }
        }
        sw_dict = {"openapi": "3.0.0", "paths": {"/items": endpoint_spec}}
        result = self.source._extract_schema_from_openapi_spec(
            "/items", "items", sw_dict
        )
        self.assertIsNone(result)
        self.assertEqual(self.source.schema_extraction_stats.from_openapi_spec, 0)
        self.assertTrue(
            any(
                f.title == "Schema Extracted With No Fields"
                for f in self.source.report.warnings
            )
        )
