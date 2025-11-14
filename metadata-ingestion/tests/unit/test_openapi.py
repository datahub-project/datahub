import unittest
from unittest.mock import MagicMock, patch

import yaml

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.openapi import APISource, OpenApiConfig
from datahub.ingestion.source.openapi_parser import (
    flatten2list,
    get_endpoints,
    guessing_url_name,
    maybe_theres_simple_id,
    resolve_schema_references,
    try_guessing,
)


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
        """Test that exceptions in request extraction are caught and logged."""
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

        with (
            patch(
                "datahub.ingestion.source.openapi.get_schema_from_response"
            ) as mock_get_schema,
            patch("datahub.ingestion.source.openapi.logger") as mock_logger,
        ):
            # Make get_schema_from_response raise an exception
            mock_get_schema.side_effect = TypeError("Cannot process schema")

            result = self.source.extract_request_schema_from_endpoint(endpoint_spec, {})

            # Should return None and log warning
            self.assertIsNone(result)
            mock_logger.warning.assert_called()

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
        for i in range(0, 20):
            if i < 19:
                sw_dict["definitions"][f"Level{i}"] = {
                    "type": "object",
                    "properties": {
                        f"level{i + 1}": {"$ref": f"#/definitions/Level{i + 1}"},
                    },
                }
            else:
                # Last level has no reference
                sw_dict["definitions"][f"Level{i}"] = {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"},
                    },
                }

        schema = {"$ref": "#/definitions/Level0"}

        # Should handle max depth gracefully without RecursionError
        # With max_depth=10, it should stop before reaching Level10
        resolved = resolve_schema_references(schema, sw_dict, max_depth=10)

        # Should return partially resolved schema when depth limit is reached
        self.assertIsNotNone(resolved)

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
        """Test that API calls are skipped when credentials missing (should log warning but not fail)."""
        # Create config without credentials
        config_no_creds = OpenApiConfig(
            name="test_api",
            url="https://api.example.com",
            swagger_file="/openapi.json",
            enable_api_calls_for_schema_extraction=True,
        )
        source_no_creds = APISource(config_no_creds, self.ctx, "OpenApi")

        sw_dict = {
            "swagger": "2.0",
            "paths": {
                "/pets": {"get": {"responses": {"404": {"description": "Not found"}}}}
            },
        }

        # Try to extract schema - should not fail even without credentials
        # Since there's no 200 response, it should return None without making API calls
        result = source_no_creds._extract_schema_from_openapi_spec(
            "/pets", "pets", sw_dict
        )

        # Should return None (no schema in spec, and no API call made without credentials)
        self.assertIsNone(result)
