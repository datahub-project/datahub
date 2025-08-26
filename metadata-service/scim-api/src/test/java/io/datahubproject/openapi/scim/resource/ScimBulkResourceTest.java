package io.datahubproject.openapi.scim.resource;

import static org.hamcrest.Matchers.*;

import io.datahubproject.openapi.scim.repositories.ScimRepositoryTestBase;
import org.apache.directory.scim.compliance.junit.EmbeddedServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EmbeddedServerExtension.class)
public class ScimBulkResourceTest extends ScimRepositoryTestBase {

  @Test
  void testBulk() {
    String payload =
        """
        {
          "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:BulkRequest"
          ],
          "Operations": [
            {
              "method": "POST",
              "path": "/Groups",
              "bulkId": "qwerty",
              "data": {
                "schemas": [
                  "urn:ietf:params:scim:schemas:core:2.0:Group"
                ],
                "displayName": "Group A",
                "members": [
                  {
                    "type": "User",
                    "value": "bulkId:user1"
                  }
                ]
              }
            },
            {
              "method": "POST",
              "path": "/Groups",
              "bulkId": "ytrewq",
              "data": {
                "schemas": [
                  "urn:ietf:params:scim:schemas:core:2.0:Group"
                ],
                "displayName": "Group B",
                "members": [
                  {
                    "type": "User",
                    "value": "bulkId:user1"
                  },
                  {
                    "type": "User",
                    "value": "bulkId:user2"
                  }
                ]
              }
            },
            {
              "method": "POST",
              "path": "/Users",
              "bulkId": "user1",
              "data": {
                "schemas": [
                  "urn:ietf:params:scim:schemas:core:2.0:User"
                ],
                "userName": "testuser1"
              }
            },
            {
              "method": "POST",
              "path": "/Users",
              "bulkId": "user2",
              "data": {
                "schemas": [
                  "urn:ietf:params:scim:schemas:core:2.0:User"
                ],
                "userName": "testuser2"
              }
            }
          ]
        }
        """;

    post("/Bulk", payload).statusCode(200);

    get("/Users/dXJuOmxpOmNvcnB1c2VyOnRlc3R1c2VyMQ==")
        .statusCode(200)
        .body(
            "groups[0].$ref", endsWith("/openapi/scim/v2/Groups/dXJuOmxpOmNvcnBHcm91cDpHcm91cCBB"),
            "groups[1].$ref", endsWith("/openapi/scim/v2/Groups/dXJuOmxpOmNvcnBHcm91cDpHcm91cCBC"));

    get("/Users/dXJuOmxpOmNvcnB1c2VyOnRlc3R1c2VyMg==")
        .statusCode(200)
        .body(
            "groups[0].$ref", endsWith("/openapi/scim/v2/Groups/dXJuOmxpOmNvcnBHcm91cDpHcm91cCBC"));
  }
}
