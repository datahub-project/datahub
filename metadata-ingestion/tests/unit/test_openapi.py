import unittest

import yaml

from datahub.ingestion.source.openapi_parser import (
    flatten2list,
    get_endpoints,
    guessing_url_name,
    maybe_theres_simple_id,
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
