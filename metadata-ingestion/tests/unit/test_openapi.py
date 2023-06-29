import unittest

import pytest
import yaml

from datahub.ingestion.source.openapi_parser import (
    SchemaMetadataExtractor,
    flatten2list,
    get_endpoints,
    guessing_url_name,
    maybe_theres_simple_id,
    try_guessing,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
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
"""

    def test_get_endpoints_openapi30(self) -> None:
        """extracting 'get' type endpoints from swagger 3.0 file"""
        sw_file_raw = yaml.safe_load(self.openapi30)
        url_endpoints = get_endpoints(sw_file_raw)

        self.assertEqual(len(url_endpoints), 2)
        d4k = {"data": "", "tags": "", "description": ""}
        self.assertEqual(url_endpoints["/"].keys(), d4k.keys())

    def test_get_endpoints_openapi20(self) -> None:
        """extracting 'get' type endpoints from swagger 2.0 file"""
        sw_file_raw = yaml.safe_load(self.openapi20)
        url_endpoints = get_endpoints(sw_file_raw)

        self.assertEqual(len(url_endpoints), 2)
        d4k = {"data": "", "tags": "", "description": ""}
        self.assertEqual(url_endpoints["/"].keys(), d4k.keys())


class TestExplodeDict(unittest.TestCase):
    def test_d1(self):
        #  exploding keys of a dict...
        d = {"a": {"b": 3}, "c": 2, "asdasd": {"ytkhj": 2, "uylkj": 3}}

        exp_l = ["a-b", "c", "asdasd-ytkhj", "asdasd-uylkj"]

        cal_l = flatten2list(d)
        self.assertEqual(exp_l, cal_l)


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


NESTED_SCHEMAS_DEFINITIONS = {
    "definitions": {
        "FirstSchema": {
            "properties": {
                "fs_first_field": {"type": "string"},
                "fs_second_field": {"type": "boolean"},
                "fs_third_field": {"items": {"type": "string"}, "type": "array"},
                "fs_fourth_field": {"format": "int32", "type": "integer"},
            },
            "type": "object",
        },
        "SecondSchema": {
            "properties": {
                "sc_first_field": {
                    "format": "int32",
                    "readOnly": True,
                    "type": "integer",
                },
                "sc_second_field": {
                    "items": {"type": "string"},
                    "readOnly": True,
                    "type": "array",
                },
                "sc_third_field": {"type": "string"},
                "sc_fourth_field": {
                    "items": {"$ref": "#/definitions/SecondSchema"},
                    "type": "array",
                },
                "sc_fifth_field": {
                    "items": {"$ref": "#/definitions/ThirdSchema"},
                    "type": "array",
                },
            },
            "required": [
                "name",
            ],
            "type": "object",
        },
        "ThirdSchema": {
            "properties": {
                "ts_first_field": {"$ref": "#/definitions/SecondSchema"},
                "ts_second_field": {"format": "int32", "type": "integer"},
                "ts_third_field": {"type": "string"},
            },
            "required": [
                "model",
            ],
            "type": "object",
        },
        "FourthSchema": {
            "properties": {
                "os_first_field": {
                    "items": {"$ref": "#/definitions/FirstSchema"},
                    "type": "array",
                },
                "os_second_field": {"$ref": "#/definitions/ThirdSchema"},
                "os_third_field": {"type": "string"},
                "oc_fourth_field": {"format": "int32", "type": "integer"},
            },
            "type": "object",
        },
    }
}

SCHEMAS_WITH_ONE_OF_DEFINITIONS = {
    "definitions": {
        "OneOfFirst": {
            "properties": {
                "one_non_common": {"type": "string"},
                "common": {"type": "boolean"},
                "common_1": {"type": "string"},
            },
            "type": "object",
        },
        "OneOfSecond": {
            "properties": {
                "two_non_common": {
                    "format": "int32",
                    "readOnly": True,
                    "type": "integer",
                },
                "common": {"type": "boolean"},
                "common_1": {"type": "string"},
            },
            "type": "object",
        },
        "OneOfThird": {"$ref": "#/definitions/OneOfFirst"},
    }
}

SCHEMAS_WITH_ONE_OF = [
    {
        "properties": {
            "one_of": {
                "description": "Contains one of",
                "oneOf": [
                    {"$ref": "#/definitions/OneOfFirst"},
                    {"$ref": "#/definitions/OneOfSecond"},
                    {"$ref": "#/definitions/OneOfThird"},
                ],
            },
            "regular": {
                "type": "string",
            },
        },
        "type": "object",
    }
]

MIXED_FLATTEN_DEFINITIONS = {
    "definitions": {
        "flatten_array": {
            "items": {},
            "maxItems": 2147483647,
            "minItems": 0,
            "type": "array",
        },
        "flatten_1": {
            "allOf": [{"$ref": "#/definitions/fatten_string"}],
        },
        "flatten_2": {
            "allOf": [{"$ref": "#/definitions/fatten_string"}],
        },
        "flatten_3": {
            "allOf": [{"$ref": "#/definitions/fatten_string"}],
        },
        "fatten_string": {"maxLength": 65535, "minLength": 0, "type": "string"},
    }
}

MIXED_FLATTEN_SCHEMAS = [
    {
        "properties": {
            "items": {
                "allOf": [
                    {
                        "items": {},
                        "maxItems": 2147483647,
                        "minItems": 0,
                        "type": "array",
                    },
                    {
                        "items": {
                            "properties": {
                                "flatten_1": {"$ref": "#/definitions/flatten_1"},
                                "flatten_2": {"$ref": "#/definitions/flatten_2"},
                            },
                            "type": "object",
                        },
                        "type": "array",
                    },
                ]
            },
            "flatten_3": {"$ref": "#/definitions/flatten_3"},
            "array": {
                "type": "array",
                "items": {"$ref": "#/definitions/flatten_string"},
            },
        },
        "type": "object",
    }
]

SCHEMAS = [
    {
        "properties": {
            "actions": {"readOnly": True},
            "id": {"type": "string"},
        },
        "type": "object",
    },
    {
        "properties": {
            "e1_f1": {"type": "string"},
            "e2_f2": {
                "items": {
                    "properties": {
                        "e1_i_f1": {"type": "boolean"},
                        "e1_i_f2": {"type": "string"},
                        "e1_i_f3": {"type": "object"},
                    },
                    "type": "object",
                },
                "type": "array",
            },
        },
        "type": "object",
    },
    {
        "properties": {
            "e2_f1": {
                "properties": {
                    "e2_i_f1": {"items": {"type": "string"}, "type": "array"},
                },
                "type": "object",
            }
        },
        "type": "object",
    },
    {
        "properties": {
            "e3_f1": {
                "items": "string",
                "type": "array",
            },
        },
        "type": "object",
    },
    {
        "items": {
            "$ref": "#/definitions/FirstSchema",
        },
        "type": "array",
    },
    {
        "properties": {
            "e5_f1": {
                "items": {"$ref": "#/definitions/MissingSchema"},
                "type": "array",
            },
        },
        "type": "object",
    },
    {
        "items": {
            "$ref": "#/definitions/MissingSchema",
            "type": "object",
        },
    },
    {
        "properties": {
            "e7_f1": {
                "items": "string",
                "type": "array",
            },
        },
        "type": "object",
    },
    {
        "properties": {
            "e8_f1": {
                "type": "array",
            },
        },
        "type": "object",
    },
    {
        "properties": {
            "e9_f1": {
                "type": "string",
            },
            "e9_f2": {
                "type": "integer",
                "format": "int64",
            },
            "e9_f3": {
                "type": "array",
                "items": {
                    "$ref": "#/components/schemas/MissingSchema",
                },
            },
        },
    },
]

FLATTEN_SCHEMAS = [
    {
        "type": "array",
        "nullable": True,
        "minItems": 0,
        "maxItems": 99,
        "items": {
            "type": "string",
            "nullable": True,
            "minLength": 11,
            "maxLength": 11,
            "example": "79151234567",
        },
    },
    {
        "format": "int32",
        "type": "integer",
    },
    {
        "format": "binary",
        "type": "string",
    },
]

EXPECTED_FIELDS = [
    [
        SchemaField(
            fieldPath="actions",
            type=SchemaFieldDataTypeClass(type=NullTypeClass()),
            nativeDataType="'unknown'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="id",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
    ],
    [
        SchemaField(
            fieldPath="e1_f1",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="e2_f2",
            type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["object"])),
            nativeDataType="'array'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="e2_f2.e1_i_f1",
            type=SchemaFieldDataTypeClass(type=BooleanTypeClass()),
            nativeDataType="'boolean'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="e2_f2.e1_i_f2",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="e2_f2.e1_i_f3",
            type=SchemaFieldDataTypeClass(type=MapTypeClass()),
            nativeDataType="'object'",
            description="",
            recursive=False,
        ),
    ],
    [
        SchemaField(
            fieldPath="e2_f1.e2_i_f1",
            type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["string"])),
            nativeDataType="'array'",
            description="",
            recursive=False,
        ),
    ],
    [
        SchemaField(
            fieldPath="e3_f1",
            type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["string"])),
            nativeDataType="'array'",
            description="",
            recursive=False,
        ),
    ],
    [
        SchemaField(
            fieldPath="fs_first_field",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="fs_second_field",
            type=SchemaFieldDataTypeClass(type=BooleanTypeClass()),
            nativeDataType="'boolean'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="fs_third_field",
            type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["string"])),
            nativeDataType="'array'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="fs_fourth_field",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
            nativeDataType="'integer'",
            description="",
            recursive=False,
        ),
    ],
    [
        SchemaField(
            fieldPath="e5_f1",
            type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["unknown"])),
            nativeDataType="'array'",
            description="",
            recursive=False,
        ),
    ],
    [],
    [
        SchemaField(
            fieldPath="e7_f1",
            type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["string"])),
            nativeDataType="'array'",
            description="",
            recursive=False,
        ),
    ],
    [
        SchemaField(
            fieldPath="e8_f1",
            type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=[""])),
            nativeDataType="'array'",
            description="",
            recursive=False,
        ),
    ],
    [
        SchemaField(
            fieldPath="e9_f1",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="e9_f2",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
            nativeDataType="'integer'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="e9_f3",
            type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["unknown"])),
            nativeDataType="'array'",
            description="",
            recursive=False,
        ),
    ],
]

NESTED_EXPECTED_FIELDS = [
    SchemaField(
        fieldPath="os_first_field",
        type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["object"])),
        nativeDataType="'array'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_first_field.fs_first_field",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="'string'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_first_field.fs_second_field",
        type=SchemaFieldDataTypeClass(type=BooleanTypeClass()),
        nativeDataType="'boolean'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_first_field.fs_third_field",
        type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["string"])),
        nativeDataType="'array'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_first_field.fs_fourth_field",
        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        nativeDataType="'integer'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_second_field.ts_first_field.sc_first_field",
        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        nativeDataType="'integer'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_second_field.ts_first_field.sc_second_field",
        type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["string"])),
        nativeDataType="'array'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_second_field.ts_first_field.sc_third_field",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="'string'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_second_field.ts_first_field.sc_fourth_field",
        type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["object"])),
        nativeDataType="'array'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_second_field.ts_first_field.sc_fifth_field",
        type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["object"])),
        nativeDataType="'array'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_second_field.ts_second_field",
        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        nativeDataType="'integer'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_second_field.ts_third_field",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="'string'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="os_third_field",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="'string'",
        description="",
        recursive=False,
    ),
    SchemaField(
        fieldPath="oc_fourth_field",
        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        nativeDataType="'integer'",
        description="",
        recursive=False,
    ),
]

FLATTEN_EXPECTED_FIELDS = [
    [],
    [
        SchemaField(
            fieldPath="",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
            nativeDataType="'integer'",
            description="",
            recursive=False,
        ),
    ],
    [
        SchemaField(
            fieldPath="",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
    ],
]

ONE_OF_EXPECTED_FIELDS = [
    [
        SchemaField(
            fieldPath="one_of.one_non_common",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="one_of.common",
            type=SchemaFieldDataTypeClass(type=BooleanTypeClass()),
            nativeDataType="'boolean'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="one_of.common_1",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="one_of.two_non_common",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
            nativeDataType="'integer'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="regular",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
    ]
]

EXPECTED_MIXED_FLATTEN = [
    [
        SchemaField(
            fieldPath="items",
            type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["object"])),
            nativeDataType="'array'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="items.flatten_1",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="items.flatten_2",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="flatten_3",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="'string'",
            description="",
            recursive=False,
        ),
        SchemaField(
            fieldPath="array",
            type=SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["unknown"])),
            nativeDataType="'array'",
            description="",
            recursive=False,
        ),
    ]
]


@pytest.mark.parametrize("schema, expected_fields", zip(SCHEMAS, EXPECTED_FIELDS))
def test_schemas_parsing(schema, expected_fields):
    metadata_extractor = SchemaMetadataExtractor("", schema, NESTED_SCHEMAS_DEFINITIONS)
    metadata_extractor.extract_metadata()
    assert metadata_extractor.canonical_schema == expected_fields


@pytest.mark.parametrize(
    "schema, expected_fields",
    (
        (
            NESTED_SCHEMAS_DEFINITIONS["definitions"]["FourthSchema"],
            NESTED_EXPECTED_FIELDS,
        ),
    ),
)
def test_parse_nested_schema(schema, expected_fields):
    metadata_extractor = SchemaMetadataExtractor("", {}, NESTED_SCHEMAS_DEFINITIONS)
    metadata_extractor.parse_schema(schema)
    assert metadata_extractor.canonical_schema == expected_fields


@pytest.mark.parametrize(
    "schema, expected_fields", zip(FLATTEN_SCHEMAS, FLATTEN_EXPECTED_FIELDS)
)
def test_parse_fatten_responses(schema, expected_fields):
    metadata_extractor = SchemaMetadataExtractor("", {}, {})
    metadata_extractor.parse_flatten_schema(schema, "", "")
    assert metadata_extractor.canonical_schema == expected_fields


@pytest.mark.parametrize(
    "schema, expected_fields", zip(SCHEMAS_WITH_ONE_OF, ONE_OF_EXPECTED_FIELDS)
)
def test_parse_with_one_of(schema, expected_fields):
    metadata_extractor = SchemaMetadataExtractor(
        "", {}, SCHEMAS_WITH_ONE_OF_DEFINITIONS
    )
    metadata_extractor.parse_schema(schema)
    assert metadata_extractor.canonical_schema == expected_fields


@pytest.mark.parametrize(
    "schema, expected_fields", zip(MIXED_FLATTEN_SCHEMAS, EXPECTED_MIXED_FLATTEN)
)
def test_parse_mixed(schema, expected_fields):
    metadata_extractor = SchemaMetadataExtractor("", {}, MIXED_FLATTEN_DEFINITIONS)
    metadata_extractor.parse_schema(schema)
    assert metadata_extractor.canonical_schema == expected_fields
