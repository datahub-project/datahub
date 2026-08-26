import json
import unittest
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import requests
import yaml
from pydantic import SecretStr, ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.extractor.json_schema_util import get_schema_metadata
from datahub.ingestion.source.openapi import (
    APISource,
    OpenApiConfig,
    OpenApiGetTokenConfig,
)
from datahub.ingestion.source.openapi_parser import (
    check_sw_version,
    flatten2list,
    get_endpoints,
    get_url_basepath,
    guessing_url_name,
    maybe_theres_simple_id,
    merge_allof_schemas,
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

    def test_check_sw_version_missing_version_logs_warning(self) -> None:
        with self.assertLogs(
            "datahub.ingestion.source.openapi_parser", level="WARNING"
        ) as cm:
            check_sw_version({"paths": {}})
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

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:
            expected_schema = {
                "type": "object",
                "properties": {
                    "id": {"type": "integer", "format": "int64"},
                    "name": {"type": "string"},
                },
            }
            mock_get_schema.return_value = expected_schema

            result = self.source.extract_response_schema_from_endpoint(
                endpoint_spec, sw_dict
            )

            self.assertIsNotNone(result)
            mock_get_schema.assert_called_once()
            self.assertEqual(result, expected_schema)

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

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:
            expected_schema = {
                "type": "object",
                "properties": {
                    "id": {"type": "integer", "format": "int64"},
                    "name": {"type": "string"},
                },
            }
            mock_get_schema.return_value = expected_schema

            result = self.source.extract_response_schema_from_endpoint(
                endpoint_spec, sw_dict
            )

            self.assertIsNotNone(result)
            mock_get_schema.assert_called_once()
            self.assertEqual(result, expected_schema)

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

        with patch(
            "datahub.ingestion.source.openapi.get_schema_from_response"
        ) as mock_get_schema:
            expected_schema = {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "tag": {"type": "string"},
                },
            }
            mock_get_schema.return_value = expected_schema

            result = self.source.extract_request_schema_from_endpoint(
                endpoint_spec, sw_dict
            )

            self.assertIsNotNone(result)
            mock_get_schema.assert_called_once()
            self.assertEqual(result, expected_schema)

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
        """Test that exceptions in response extraction are caught and logged."""
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

        with (
            patch(
                "datahub.ingestion.source.openapi.get_schema_from_response"
            ) as mock_get_schema,
            patch("datahub.ingestion.source.openapi.logger") as mock_logger,
        ):
            # Make get_schema_from_response raise an exception
            mock_get_schema.side_effect = TypeError("Cannot process schema")

            result = self.source.extract_response_schema_from_endpoint(
                endpoint_spec, {}
            )

            # Should return None and log warning
            self.assertIsNone(result)
            mock_logger.warning.assert_called()

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
        self.assertEqual(resolved.get("$ref"), "#/components/schemas/Missing")
        self.assertTrue(
            any("Unable to resolve schema $ref" in msg for msg in cm.output)
        )

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
        self.assertTrue(
            any(
                getattr(f, "title", None) == "Failed to Create Schema Metadata"
                for f in self.source.report.failures
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
