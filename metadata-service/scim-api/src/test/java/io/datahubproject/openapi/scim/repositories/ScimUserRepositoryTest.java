package io.datahubproject.openapi.scim.repositories;

import static org.hamcrest.Matchers.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.CorpuserUrn;
import io.restassured.RestAssured;
import io.restassured.config.RestAssuredConfig;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import org.apache.directory.scim.compliance.junit.EmbeddedServerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EmbeddedServerExtension.class)
public class ScimUserRepositoryTest extends ScimRepositoryTestBase {
  String userName1 = "scimTestUser@example.com";

  String userId1 =
      Base64.getUrlEncoder().encodeToString(("urn:li:corpuser:" + userName1).getBytes());

  @AfterEach
  public void deleteUserIfExists() {
    delete("/Users/" + userId1);
  }

  @Test
  public void testBasicCRUD() throws Exception {
    String createBody =
        String.format(
            """
                {
                  "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                  "userName": "%s"
                }
                """,
            userName1);

    long start = System.currentTimeMillis();

    ValidatableResponse validatableResponse = post("/Users", createBody);
    validatableResponse
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "active", is(true),
            "userName", is(userName1),
            "id", is(userId1),
            "meta.resourceType", is("User"),
            "meta.created", isTsLatency(start),
            "meta.lastModified", isTsLatency(start),
            "meta.location", endsWith("openapi/scim/v2/Users/" + userId1),
            "externalId", nullValue());

    String created = validatableResponse.extract().body().path(CREATED);

    validatableResponse =
        get("/Users/" + userId1)
            .statusCode(200)
            .body(
                "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
                "active", is(true),
                "userName", is(userName1),
                "id", is(userId1),
                "meta.resourceType", is("User"),
                "meta.created", is(created),
                "meta.lastModified", isTsSys(lastModifiedTs(validatableResponse)),
                "meta.location", endsWith("openapi/scim/v2/Users/" + userId1),
                "externalId", nullValue());

    String putBody =
        """
        {
          "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
          "name": {
               "familyName": "famNm1",
               "givenName": "givNm1"
          }
        }
        """;

    validatableResponse =
        put("/Users/" + userId1, putBody)
            .statusCode(200)
            .body(
                "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
                "active", is(true),
                "userName", is(userName1),
                "id", is(userId1),
                "meta.resourceType", is("User"),
                "meta.created", is(created),
                "meta.lastModified", isTsLatency(lastModifiedTs(validatableResponse)),
                "meta.location", endsWith("openapi/scim/v2/Users/" + userId1),
                "externalId", nullValue(),
                "name.familyName", is("famNm1"),
                "name.givenName", is("givNm1"));

    validatableResponse =
        get("/Users/" + userId1)
            .statusCode(200)
            .body(
                "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
                "active", is(true),
                "userName", is(userName1),
                "id", is(userId1),
                "meta.resourceType", is("User"),
                "meta.created", is(created),
                "meta.lastModified", isTsSys(lastModifiedTs(validatableResponse)),
                "meta.location", endsWith("openapi/scim/v2/Users/" + userId1),
                "externalId", nullValue(),
                "name.familyName", is("famNm1"),
                "name.givenName", is("givNm1"));

    // check GET consistency
    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "active", is(true),
            "userName", is(userName1),
            "id", is(userId1),
            "meta.resourceType", is("User"),
            "meta.created", is(created),
            "meta.lastModified",
                is(validatableResponse.extract().body().path(LAST_MODIFIED).toString()),
            "meta.location", endsWith("openapi/scim/v2/Users/" + userId1),
            "externalId", nullValue(),
            "name.familyName", is("famNm1"),
            "name.givenName", is("givNm1"));

    delete("/Users/" + userId1).statusCode(204);

    get("/Users/" + userId1).statusCode(404).body("detail", is("User " + userId1 + " not found."));

    delete("/Users/" + userId1)
        .statusCode(404)
        .body("detail", is("User " + userId1 + " not found."));

    put("/Users/" + userId1, putBody)
        .statusCode(404)
        .body("detail", is("User " + userId1 + " not found."));
  }

  @Test
  void testDuplicateCreation() {

    String createBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s"
            }
            """,
            userName1);

    post("/Users", createBody).statusCode(201);

    post("/Users", createBody)
        .statusCode(409)
        .body("detail", is("Resource with name " + userName1 + " already exists."));

    delete("/Users/" + userId1).statusCode(204);
    post("/Users", createBody).statusCode(201);
  }

  @Test
  void testKeyChange() {
    String body =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s"
            }
            """,
            userName1);

    post("/Users", body).statusCode(201);

    body =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s1"
            }
            """,
            userName1);

    put("/Users/" + userId1, body)
        .statusCode(400)
        .body(
            "detail",
            is(String.format("userName cannot be modified. %s => %s1", userName1, userName1)));
  }

  @Test
  public void testQueryUserName() {
    String userName2 = "scimTestUser2@example.com";
    String userId2 =
        Base64.getUrlEncoder().encodeToString(("urn:li:corpuser:" + userName2).getBytes());

    post(
        "/Users",
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s"
            }
            """,
            userName1));

    post(
        "/Users",
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s"
            }
            """,
            userName2));

    get("/Users", ImmutableMap.of("filter", "userName Eq \"" + userName1 + "\""))
        .statusCode(200)
        .body(
            "totalResults", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "Resources[0].userName", is(userName1),
            "Resources[0].id", is(userId1),
            "Resources[0].meta.resourceType", is("User"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Users/" + userId1));

    get("/Users", ImmutableMap.of("filter", "userName Eq \"" + userName2 + "\""))
        .statusCode(200)
        .body(
            "totalResults", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "Resources[0].userName", is(userName2),
            "Resources[0].id", is(userId2),
            "Resources[0].meta.resourceType", is("User"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Users/" + userId2));

    get("/Users", ImmutableMap.of("filter", "userName Eq \"" + userName1 + "\" or foo Eq \"bar\""))
        .statusCode(200)
        .body(
            "totalResults", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "Resources[0].userName", is(userName1),
            "Resources[0].id", is(userId1),
            "Resources[0].meta.resourceType", is("User"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Users/" + userId1));

    get("/Users", ImmutableMap.of("filter", "userName Eq \"notExists\""))
        .statusCode(200)
        .body("totalResults", is(0));

    delete("/Users/" + userId2);

    get("/Users", ImmutableMap.of("filter", "userName Eq \"" + userName2 + "\""))
        .statusCode(200)
        .body("totalResults", is(0));
  }

  @Test
  public void testQueryUnsupportedFilter() {
    get("/Users", ImmutableMap.of("filter", "displayName Eq \"" + userName1 + "\""))
        .statusCode(400)
        .body("detail", is("Unparsable filter: displayName EQ \"scimTestUser@example.com\""));

    get("/Users", ImmutableMap.of("filter", "userName Eq \"" + userName1 + "\" and foo Eq \"bar\""))
        .statusCode(400)
        .body(
            "detail",
            is("Unparsable filter: userName EQ \"scimTestUser@example.com\" AND foo EQ \"bar\""));
  }

  @Test
  public void testPagination() {
    String userName2 = "scimTestUser2@example.com";
    String userId2 =
        Base64.getUrlEncoder().encodeToString(("urn:li:corpuser:" + userName2).getBytes());

    post(
        "/Users",
        String.format(
            """
                    {
                      "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                      "userName": "%s"
                    }
                    """,
            userName1));

    post(
        "/Users",
        String.format(
            """
                    {
                      "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                      "userName": "%s"
                    }
                    """,
            userName2));

    get("/Users", ImmutableMap.of("startIndex", "1", "count", "10"))
        .statusCode(200)
        .body(
            "totalResults", is(2),
            "startIndex", is(1),
            "itemsPerPage", is(2),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[1].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "Resources[1].userName", is(userName1),
            "Resources[1].id", is(userId1),
            "Resources[1].meta.resourceType", is("User"),
            "Resources[1].meta.location", endsWith("openapi/scim/v2/Users/" + userId1),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "Resources[0].userName", is(userName2),
            "Resources[0].id", is(userId2),
            "Resources[0].meta.resourceType", is("User"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Users/" + userId2));

    get("/Users", ImmutableMap.of("startIndex", "2", "count", "10"))
        .statusCode(200)
        .body(
            "totalResults", is(2),
            "startIndex", is(2),
            "itemsPerPage", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "Resources[0].userName", is(userName1),
            "Resources[0].id", is(userId1),
            "Resources[0].meta.resourceType", is("User"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Users/" + userId1));

    get("/Users", ImmutableMap.of("startIndex", "1", "count", "1"))
        .statusCode(200)
        .body(
            "totalResults", is(2),
            "startIndex", is(1),
            "itemsPerPage", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "Resources[0].userName", is(userName2),
            "Resources[0].id", is(userId2),
            "Resources[0].meta.resourceType", is("User"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Users/" + userId2));

    get("/Users", ImmutableMap.of("startIndex", "3", "count", "10"))
        .statusCode(200)
        .body("totalResults", is(0));

    delete("/Users/" + userId2);

    get("/Users", ImmutableMap.of("startIndex", "1", "count", "10"))
        .statusCode(200)
        .body(
            "totalResults", is(1),
            "startIndex", is(1),
            "itemsPerPage", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "Resources[0].userName", is(userName1),
            "Resources[0].id", is(userId1),
            "Resources[0].meta.resourceType", is("User"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Users/" + userId1));

    get("/Users", ImmutableMap.of("startIndex", "2", "count", "10"))
        .statusCode(200)
        .body("totalResults", is(0));
  }

  @Test
  public void testFullUserResource() {
    // adapted from the example at https://datatracker.ietf.org/doc/html/rfc7643#section-8.2
    String createBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s",
              "name": {
                "formatted": "Ms. Barbara J Jensen, III",
                "familyName": "Jensen",
                "givenName": "Barbara"
              },
              "displayName": "Babs Jensen",
              "emails": [
                {
                  "value": "bjensen@example.com",
                  "type": "work",
                  "primary": true
                },
                {
                  "value": "babs@jensen.org",
                  "type": "home"
                }
              ],
              "phoneNumbers": [
                {
                  "value": "555-555-5555",
                  "type": "work"
                },
                {
                  "value": "555-555-4444",
                  "type": "mobile",
                  "primary": true
                }
              ],
              "photos": [
                {
                  "value":
                    "https://photos.example.com/profilephoto/72930000000Ccne/F",
                  "type": "photo"
                },
                {
                  "value":
                    "https://photos.example.com/profilephoto/72930000000Ccne/T",
                  "type": "thumbnail"
                }
              ],
              "title": "Tour Guide",
              "active":true
            }
            """,
            userName1);

    post("/Users", createBody)
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "name.formatted", is("Ms. Barbara J Jensen, III"),
            "name.familyName", is("Jensen"),
            "name.givenName", is("Barbara"),
            "displayName", is("Babs Jensen"),
            "emails.size()", is(1),
            "emails[0].value", is("bjensen@example.com"),
            "phoneNumbers.size()", is(1),
            "phoneNumbers[0].value", is("555-555-4444"),
            "photos.size()", is(1),
            "photos[0].value", is("https://photos.example.com/profilephoto/72930000000Ccne/F"),
            "title", is("Tour Guide"),
            "active", is(true));

    // an update and then a get
    // modify title, remove photos, retain emails/phones and other info
    put(
            "/Users/" + userId1,
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "title": "Managing Director",
              "emails": [
                {
                  "value": "bjensen@example.com",
                  "type": "work",
                  "primary": true
                },
                {
                  "value": "babs@jensen.org",
                  "type": "home"
                }
              ],
              "phoneNumbers": [
                {
                  "value": "555-555-5555",
                  "type": "work"
                },
                {
                  "value": "555-555-4444",
                  "type": "mobile",
                  "primary": true
                }
              ]
            }
            """)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "name.formatted", is("Ms. Barbara J Jensen, III"),
            "name.familyName", is("Jensen"),
            "name.givenName", is("Barbara"),
            "displayName", is("Babs Jensen"),
            "emails.size()", is(1),
            "emails[0].value", is("bjensen@example.com"),
            "phoneNumbers.size()", is(1),
            "phoneNumbers[0].value", is("555-555-4444"),
            "photos", nullValue(),
            "title", is("Managing Director"),
            "active", is(true));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "name.formatted", is("Ms. Barbara J Jensen, III"),
            "name.familyName", is("Jensen"),
            "name.givenName", is("Barbara"),
            "displayName", is("Babs Jensen"),
            "emails.size()", is(1),
            "emails[0].value", is("bjensen@example.com"),
            "phoneNumbers.size()", is(1),
            "phoneNumbers[0].value", is("555-555-4444"),
            "photos", nullValue(),
            "title", is("Managing Director"),
            "active", is(true));

    // a patch and get
    String patchBody =
        """
            {
              "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
              "Operations": [
                {
                  "op":"add",
                  "path":"emails",
                  "value":[
                    {
                      "value": "bjensen2@example.com"
                    }
                  ]
                },
                {
                  "op":"replace",
                  "path":"title",
                  "value": "Tour Director"
                },
                {
                  "op":"remove",
                  "path":"phoneNumbers",
                  "value":[
                    {
                      "value": "555-555-4444"
                    }
                  ]
                }
              ]
            }
            """;

    // email wouldn't have changed in our system
    // phoneNumbers would now be empty
    // title would've changed
    patch("/Users/" + userId1, patchBody)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "name.formatted", is("Ms. Barbara J Jensen, III"),
            "name.familyName", is("Jensen"),
            "name.givenName", is("Barbara"),
            "displayName", is("Babs Jensen"),
            "emails.size()", is(1),
            "emails[0].value", is("bjensen@example.com"),
            "phoneNumbers", nullValue(),
            "photos", nullValue(),
            "title", is("Tour Director"),
            "active", is(true));
  }

  @Test
  public void testUserStatus() {
    post(
            "/Users",
            String.format(
                """
                {
                  "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                  "userName": "%s",
                  "active": true
                }
                """,
                userName1))
        .statusCode(201);

    put(
            "/Users/" + userId1,
            """
                {
                  "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                  "active": false
                }
                """)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "active", is(false),
            "userName", is(userName1),
            "id", is(userId1));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "active", is(false),
            "userName", is(userName1),
            "id", is(userId1));

    put(
            "/Users/" + userId1,
            """
                {
                  "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                  "active": true
                }
                """)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "active", is(true),
            "userName", is(userName1),
            "id", is(userId1));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "active", is(true),
            "userName", is(userName1),
            "id", is(userId1));
  }

  @Test
  public void testRolesCreateUpdate() {
    post(
            "/Users",
            String.format(
                """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s",
              "roles": [
                  {"value": "Admin"}
                ]
            }
            """,
                userName1))
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Admin"),
            "roles[0].primary", is(true));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Admin"),
            "roles[0].primary", is(true));

    /*
     check two valid roles
     (the specific examples of Editor-Reader are not too sensible,
     but the test case is to extend to arbitrary valid roles in future.)
    */
    put(
            "/Users/" + userId1,
            """
        {
          "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
          "roles": [
              {"value": "Editor"},
              {"value": "Reader"}
            ]
        }
        """)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(2),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true),
            "roles[1].value", is("Reader"),
            "roles[1].primary", is(false));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(2),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true),
            "roles[1].value", is("Reader"),
            "roles[1].primary", is(false));

    patch(
            "/Users/" + userId1,
            """
              {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [
                  {
                  "op":"replace",
                  "path":"roles",
                  "value":[
                      {
                        "value": "Admin"
                      }
                    ]
                  }]
              }
          """)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Admin"),
            "roles[0].primary", is(true));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Admin"),
            "roles[0].primary", is(true));

    patch(
            "/Users/" + userId1,
            """
            {
              "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
              "Operations": [
                {
                "op":"add",
                "path":"roles",
                "value":[
                    {
                      "value": "Editor"
                    },
                    {
                      "value": "Reader"
                    }
                  ]
                },
                {
                "op":"remove",
                "path":"roles",
                "value":[
                    {
                      "value": "Admin"
                    }
                  ]
                }]
            }
        """)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(2),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true),
            "roles[1].value", is("Reader"),
            "roles[1].primary", is(false));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(2),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true),
            "roles[1].value", is("Reader"),
            "roles[1].primary", is(false));
  }

  @Test
  public void testRoleInvalidCreateUpdate() {
    post(
            "/Users",
            String.format(
                """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s",
              "roles": [
                  {"value": "FooRole"}
                ]
            }
            """,
                userName1))
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles", nullValue());

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles", nullValue());

    put(
            "/Users/" + userId1,
            String.format(
                """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "roles": [
                  {"value": "FooRole"},
                  {"value": "Editor"}
                ]
            }
            """,
                userName1))
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true));

    patch(
            "/Users/" + userId1,
            """
            {
              "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
              "Operations": [
                {
                "op":"add",
                "path":"roles",
                "value":[
                    {
                      "value": "FooRole"
                    }
                  ]
                }]
            }
        """)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true));
  }

  @Test
  public void testPrimaryRole() {
    // check that the primary role is maintained at first position of role-membership
    post(
            "/Users",
            String.format(
                """
    {
      "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
      "userName": "%s",
      "roles": [
          {"value": "Admin"},
          {"value": "Editor", "primary": true}
        ]
    }
    """,
                userName1))
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(2),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true),
            "roles[1].value", is("Admin"),
            "roles[1].primary", is(false));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(2),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true),
            "roles[1].value", is("Admin"),
            "roles[1].primary", is(false));

    patch(
            "/Users/" + userId1,
            """
      {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [
          {
          "op":"add",
          "path":"roles[primary EQ true].value",
          "value":"Reader"
          }
        ]
      }
    """)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(2),
            "roles[0].value", is("Reader"),
            "roles[0].primary", is(true),
            "roles[1].value", is("Admin"),
            "roles[1].primary", is(false));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(2),
            "roles[0].value", is("Reader"),
            "roles[0].primary", is(true),
            "roles[1].value", is("Admin"),
            "roles[1].primary", is(false));

    put(
            "/Users/" + userId1,
            String.format(
                """
        {
          "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
          "roles": [
              {"value": "Admin", "primary": true},
              {"value": "Editor"}
            ]
        }
        """,
                userName1))
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles[0].value", is("Admin"),
            "roles[0].primary", is(true),
            "roles[1].value", is("Editor"),
            "roles[1].primary", is(false));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(2),
            "roles[0].value", is("Admin"),
            "roles[0].primary", is(true),
            "roles[1].value", is("Editor"),
            "roles[1].primary", is(false));

    patch(
            "/Users/" + userId1,
            """
      {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [
            {
            "op":"remove",
            "path":"roles",
            "value":[
                {
                  "value": "Admin",
                  "primary": true
                }
              ]
            }
        ]
      }
    """)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true));
  }

  @Test
  public void testExternalId() throws Exception {
    post(
            "/Users",
            String.format(
                """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s",
              "externalId": "someExternalId1"
            }
            """,
                userName1))
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "externalId", is("someExternalId1"));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "externalId", is("someExternalId1"));

    patch(
            "/Users/" + userId1,
            """
            {
              "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
              "Operations": [
                {
                "op":"add",
                "path":"externalId",
                "value":"someExternalId2"
              }]
            }
        """)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "externalId", is("someExternalId2"));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "externalId", is("someExternalId2"));

    put(
            "/Users/" + userId1,
            String.format(
                """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s",
              "externalId": "someExternalId3"
            }
            """,
                userName1))
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "externalId", is("someExternalId3"));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "externalId", is("someExternalId3"));
  }

  @Test
  public void testEntraRoleUpdate() {
    // PatchOperation(operation=ADD, path=roles[primary EQ "True"].value, value=Editor)
    post(
            "/Users",
            String.format(
                """
        {
          "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
          "userName": "%s",
          "roles": [
              {"value": "Admin"}
            ]
        }
        """,
                userName1))
        .statusCode(201);

    patch(
            "/Users/" + userId1,
            """
          {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [
              {
              "op":"add",
              "path":"roles[primary EQ \\"True\\"].value",
              "value":"Editor"
              }
            ]
          }
      """)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "roles.size()", is(1),
            "roles[0].value", is("Editor"),
            "roles[0].primary", is(true));
  }

  /*
  Tests for aspects in Datahub that are not exposed in SCIM e.g. Origin, Credentials etc.
   */
  @EmbeddedServerExtension.ScimServerUri
  private URI uri = URI.create("http://localhost:8080/openapi");

  private final RestAssuredConfig config =
      RestAssured.config()
          .logConfig(
              RestAssured.config()
                  .getLogConfig()
                  .enableLoggingOfRequestAndResponseIfValidationFails());

  // As expected, this doesn't work
  //  @Autowired
  //  EntityService entityService;

  /* The following test case isn't working;
  on hold as it's tested manually and possibly isn't crucial for SCIM functionality currently */

  // @Test
  public void testOrigin() throws URISyntaxException {
    String externalId = "externalId1";
    post(
        "/Users",
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s",
              "externalId": "%s"
            }
            """,
            userName1, externalId));

    CorpuserUrn urn = new CorpuserUrn(userName1);

    // openapi
    /*
        URI newUri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
            "/openapi/entities/v1/latest",
            String.format("urns=%s&aspectNames=origin", urn), null);
    */
    // restli
    URI newUri =
        new URI(
            uri.getScheme(),
            uri.getUserInfo(),
            uri.getHost(),
            uri.getPort(),
            "/entitiesV2/" + urn,
            null,
            null);

    ValidatableResponse validatableResponse =
        (ValidatableResponse)
            ((ValidatableResponse)
                ((Response)
                        RestAssured.given()
                            .config(this.config)
                            .urlEncodingEnabled(false)
                            .redirects()
                            .follow(false)
                            .when()
                            .get(newUri))
                    .then());

    validatableResponse.log();

    validatableResponse
        .statusCode(200)
        .body(
            "urn", is(urn.toString()),
            "aspects.origin.value.type", is("EXTERNAL"),
            "aspects.origin.value.externalType", is("SCIM_client" + externalId));
    /*
       Origin origin = (Origin) entityService.getLatestAspect(urn, ORIGIN_ASPECT_NAME);
       assertNotNull(origin);
       assertEquals(OriginType.EXTERNAL, origin.getType());
       assertEquals("SCIM_client" + externalId, origin.getExternalType());
    */
  }
}
