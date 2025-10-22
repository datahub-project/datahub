package io.datahubproject.openapi.scim.repositories;

import static org.hamcrest.Matchers.*;

import com.google.common.collect.ImmutableMap;
import io.restassured.response.ValidatableResponse;
import java.util.Base64;
import org.apache.directory.scim.compliance.junit.EmbeddedServerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EmbeddedServerExtension.class)
public class ScimGroupRepositoryTest extends ScimRepositoryTestBase {
  String groupName1 = "group1";
  String groupId1 =
      Base64.getUrlEncoder().encodeToString(("urn:li:corpGroup:" + groupName1).getBytes());

  String groupName2 = "group2";
  String groupId2 =
      Base64.getUrlEncoder().encodeToString(("urn:li:corpGroup:" + groupName2).getBytes());

  String userName1 = "scimTestUser1@example.com";
  String userId1 =
      Base64.getUrlEncoder().encodeToString(("urn:li:corpuser:" + userName1).getBytes());

  String userName2 = "scimTestUser2@example.com";
  String userId2 =
      Base64.getUrlEncoder().encodeToString(("urn:li:corpuser:" + userName2).getBytes());

  @AfterEach
  public void deleteUsersGroupIfExists() {
    delete("/Users/" + userId1);
    delete("/Users/" + userId2);
    delete("/Groups/" + groupId1);
    delete("/Groups/" + groupId1);
  }

  @Test
  public void testBasicCRD() throws Exception {
    String createBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s",
              "members": []
            }
            """,
            groupName1);

    long start = System.currentTimeMillis();

    ValidatableResponse validatableResponse = post("/Groups", createBody);

    validatableResponse
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "displayName", is(groupName1),
            "id", is(groupId1),
            "members", nullValue(),
            "meta.resourceType", is("Group"),
            "meta.created", isTsLatency(start),
            "meta.lastModified", isTsLatency(start),
            "meta.location", endsWith("openapi/scim/v2/Groups/" + groupId1),
            "externalId", nullValue());

    String created = validatableResponse.extract().body().path(CREATED);

    validatableResponse =
        get("/Groups/" + groupId1)
            .statusCode(200)
            .body(
                "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
                "displayName", is(groupName1),
                "id", is(groupId1),
                "members", nullValue(),
                "meta.resourceType", is("Group"),
                "meta.created", is(created),
                "meta.lastModified", isTsSys(lastModifiedTs(validatableResponse)),
                "meta.location", endsWith("openapi/scim/v2/Groups/" + groupId1),
                "externalId", nullValue());

    // check GET consistency
    validatableResponse =
        get("/Groups/" + groupId1)
            .statusCode(200)
            .body(
                "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
                "displayName", is(groupName1),
                "id", is(groupId1),
                "members", nullValue(),
                "meta.resourceType", is("Group"),
                "meta.created", is(created),
                "meta.lastModified",
                    is(validatableResponse.extract().body().path(LAST_MODIFIED).toString()),
                "meta.location", endsWith("openapi/scim/v2/Groups/" + groupId1),
                "externalId", nullValue());

    delete("/Groups/" + groupId1).statusCode(204);

    get("/Groups/" + groupId1)
        .statusCode(404)
        .body("detail", is("Group " + groupId1 + " not found."));

    delete("/Groups/" + groupId1)
        .statusCode(404)
        .body("detail", is("Group " + groupId1 + " not found."));

    String putBody =
        """
          { "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"] }
        """;

    put("/Groups/" + groupId1, putBody)
        .statusCode(404)
        .body("detail", is("Group " + groupId1 + " not found."));
  }

  @Test
  public void testSingleGroupUpdateMembers() throws Exception {
    String createBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s"
            }
            """,
            groupName1);

    ValidatableResponse validatableResponse = post("/Groups", createBody);

    validatableResponse
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "displayName", is(groupName1),
            "id", is(groupId1),
            "members", nullValue());

    String created = validatableResponse.extract().body().path(CREATED);

    String userCreateBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s"
            }
            """,
            userName1);

    ValidatableResponse userCreateResponse = post("/Users", userCreateBody);
    String userCreated = userCreateResponse.extract().body().path(CREATED);

    // Create the second user
    String user2CreateBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s"
            }
            """,
            userName2);

    ValidatableResponse user2CreateResponse = post("/Users", user2CreateBody);
    user2CreateResponse
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName2),
            "id", is(userId2),
            "meta.resourceType", is("User"));

    String patchAddBody =
        String.format(
            """
                {
                  "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                  "Operations": [
                    {
                    "op":"add",
                    "path":"members",
                    "value":[
                      {
                        "type": "User",
                        "value": "%s"
                      }
                    ]
                  }]
                }
            """,
            userId1);

    validatableResponse =
        patch("/Groups/" + groupId1, patchAddBody)
            .statusCode(200)
            .body(
                "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
                "displayName", is(groupName1),
                "id", is(groupId1),
                "members[0].value", is(userId1),
                "members[0].$ref", endsWith("openapi/scim/v2/Users/" + userId1),
                "members.size()", is(1),
                "meta.resourceType", is("Group"),
                "meta.created", is(created),
                "meta.lastModified", isTsLatency(lastModifiedTs(validatableResponse)),
                "meta.location", endsWith("openapi/scim/v2/Groups/" + groupId1));

    // last modified of user should not have changed.
    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "userName", is(userName1),
            "id", is(userId1),
            "groups[0].value", is(groupId1),
            "groups[0].$ref", endsWith("openapi/scim/v2/Groups/" + groupId1),
            "groups.size()", is(1),
            "meta.resourceType", is("User"),
            "meta.created", is(userCreated),
            "meta.lastModified", isTsSys(lastModifiedTs(userCreateResponse)),
            "meta.location", endsWith("openapi/scim/v2/Users/" + userId1));

    get("/Groups/" + groupId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "displayName", is(groupName1),
            "id", is(groupId1),
            "members[0].value", is(userId1),
            "members[0].$ref", endsWith("openapi/scim/v2/Users/" + userId1),
            "members.size()", is(1),
            "meta.resourceType", is("Group"),
            "meta.created", is(created),
            "meta.lastModified", isTsLatency(lastModifiedTs(validatableResponse)),
            "meta.location", endsWith("openapi/scim/v2/Groups/" + groupId1));

    // patch-add another user
    // don't specify "type", it should still be ingested
    patchAddBody =
        String.format(
            """
                {
                  "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                  "Operations": [
                    {
                    "op":"add",
                    "path":"members",
                    "value":[
                      {
                        "value": "%s"
                      }
                    ]
                  }]
                }
            """,
            userId2);

    validatableResponse =
        patch("/Groups/" + groupId1, patchAddBody)
            .statusCode(200)
            .body(
                "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
                "displayName", is(groupName1),
                "id", is(groupId1),
                "members[0].value", is(userId1),
                "members[0].$ref", endsWith("openapi/scim/v2/Users/" + userId1),
                "members[1].value", is(userId2),
                "members[1].$ref", endsWith("openapi/scim/v2/Users/" + userId2),
                "members.size()", is(2));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "userName", is(userName1),
            "id", is(userId1),
            "groups[0].value", is(groupId1),
            "groups[0].$ref", endsWith("openapi/scim/v2/Groups/" + groupId1),
            "groups.size()", is(1));

    get("/Users/" + userId2)
        .statusCode(200)
        .body(
            "userName", is(userName2),
            "id", is(userId2),
            "groups[0].value", is(groupId1),
            "groups[0].$ref", endsWith("openapi/scim/v2/Groups/" + groupId1),
            "groups.size()", is(1));

    String patchRemoveBody =
        String.format(
            """
                {
                  "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                  "Operations": [
                    {
                    "op":"remove",
                    "path":"members",
                    "value":[
                      {
                        "type": "User",
                        "value": "%s"
                      }
                    ]
                  }]
                }
            """,
            userId1);

    validatableResponse =
        patch("/Groups/" + groupId1, patchRemoveBody)
            .statusCode(200)
            .body(
                "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
                "displayName", is(groupName1),
                "id", is(groupId1),
                "members[0].value", is(userId2),
                "members[0].$ref", endsWith("openapi/scim/v2/Users/" + userId2),
                "members.size()", is(1));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "userName", is(userName1),
            "id", is(userId1),
            "groups.size()", is(0));

    get("/Users/" + userId2)
        .statusCode(200)
        .body(
            "userName", is(userName2),
            "id", is(userId2),
            "groups[0].value", is(groupId1),
            "groups[0].$ref", endsWith("openapi/scim/v2/Groups/" + groupId1),
            "groups.size()", is(1));

    // include one member without "type", one member of wrong type "Group", one member of invalid
    // id. These three should get ignored.
    String putBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "members": [
                   {
                     "value": "%s"
                   },
                   {
                     "type": "User",
                     "value": "%s"
                   },
                   {
                     "type": "User",
                     "value": "ShouldBeIgnored"
                   },
                   {
                     "type": "Group",
                     "value": "ShouldBeIgnored"
                   }
                ]
            }
            """,
            userId1, userId2);

    validatableResponse =
        put("/Groups/" + groupId1, putBody)
            .statusCode(200)
            .body(
                "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
                "displayName", is(groupName1),
                "id", is(groupId1),
                "members[0].value", is(userId1),
                "members[0].$ref", endsWith("openapi/scim/v2/Users/" + userId1),
                "members[1].value", is(userId2),
                "members[1].$ref", endsWith("openapi/scim/v2/Users/" + userId2),
                "members.size()", is(2),
                "meta.resourceType", is("Group"),
                "meta.created", is(created),
                "meta.lastModified", isTsLatency(lastModifiedTs(validatableResponse)),
                "meta.location", endsWith("openapi/scim/v2/Groups/" + groupId1));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "userName", is(userName1),
            "id", is(userId1),
            "groups[0].value", is(groupId1),
            "groups[0].$ref", endsWith("openapi/scim/v2/Groups/" + groupId1),
            "groups.size()", is(1));

    get("/Users/" + userId2)
        .statusCode(200)
        .body(
            "userName", is(userName2),
            "id", is(userId2),
            "groups[0].value", is(groupId1),
            "groups[0].$ref", endsWith("openapi/scim/v2/Groups/" + groupId1),
            "groups.size()", is(1));

    delete("/Groups/" + groupId1).statusCode(204);

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "userName", is(userName1),
            "id", is(userId1),
            "groups.size()", is(0));

    get("/Users/" + userId2)
        .statusCode(200)
        .body(
            "userName", is(userName2),
            "id", is(userId2),
            "groups.size()", is(0));
  }

  @Test
  void testOneUserTwoGroups() {
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
            "/Groups",
            String.format(
                """
                {
                  "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
                  "displayName": "%s",
                  "members": [
                       {
                        "value": "%s"
                       }
                    ]
                }
                """,
                groupName1, userId1))
        .statusCode(201)
        .body(
            "members[0].value", is(userId1),
            "members[0].$ref", endsWith("openapi/scim/v2/Users/" + userId1),
            "members.size()", is(1));

    post(
        "/Groups",
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s"
            }
            """,
            groupName2));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "userName", is(userName1),
            "id", is(userId1),
            "groups[0].value", is(groupId1),
            "groups[0].$ref", endsWith("openapi/scim/v2/Groups/" + groupId1),
            "groups.size()", is(1));

    String patchAddBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
              "Operations": [{
                "op":"add",
                "path":"members",
                "value":[
                  {
                    "type": "User",
                    "value": "%s"
                  }
                ]
              }]
            }
            """,
            userId1);

    patch("/Groups/" + groupId2, patchAddBody)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "displayName", is(groupName2),
            "id", is(groupId2),
            "members[0].value", is(userId1),
            "members[0].$ref", endsWith("openapi/scim/v2/Users/" + userId1),
            "members.size()", is(1));

    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "userName", is(userName1),
            "id", is(userId1),
            "groups[0].value", is(groupId1),
            "groups[0].$ref", endsWith("openapi/scim/v2/Groups/" + groupId1),
            "groups[1].value", is(groupId2),
            "groups[1].$ref", endsWith("openapi/scim/v2/Groups/" + groupId2),
            "groups.size()", is(2));

    delete("/Groups/" + groupId1).statusCode(204);
    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "userName", is(userName1),
            "id", is(userId1),
            "groups[0].value", is(groupId2),
            "groups[0].$ref", endsWith("openapi/scim/v2/Groups/" + groupId2),
            "groups.size()", is(1));
  }

  @Test
  void testDuplicateCreation() {

    String createBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s"
            }
            """,
            groupName1);

    post("/Groups", createBody).statusCode(201);

    post("/Groups", createBody)
        .statusCode(409)
        .body("detail", is("Resource with name " + groupName1 + " already exists."));

    delete("/Groups/" + groupId1).statusCode(204);

    post("/Groups", createBody).statusCode(201);
  }

  @Test
  void testKeyChange() {

    String body =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s"
            }
            """,
            groupName1);

    post("/Groups", body).statusCode(201);

    body =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s1"
            }
            """,
            groupName1);

    put("/Groups/" + groupId1, body)
        .statusCode(400)
        .body(
            "detail",
            is(String.format("groupName cannot be modified. %s => %s1", groupName1, groupName1)));
  }

  @Test
  public void testQueryGroupName() {
    post(
        "/Groups",
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s"
            }
            """,
            groupName1));

    post(
        "/Groups",
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s"
            }
            """,
            groupName2));

    get("/Groups", ImmutableMap.of("filter", "displayName Eq \"" + groupName1 + "\""))
        .statusCode(200)
        .body(
            "totalResults", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "Resources[0].displayName", is(groupName1),
            "Resources[0].id", is(groupId1),
            "Resources[0].meta.resourceType", is("Group"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Groups/" + groupId1));

    get("/Groups", ImmutableMap.of("filter", "displayName Eq \"" + groupName2 + "\""))
        .statusCode(200)
        .body(
            "totalResults", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "Resources[0].displayName", is(groupName2),
            "Resources[0].id", is(groupId2),
            "Resources[0].meta.resourceType", is("Group"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Groups/" + groupId2));

    get(
            "/Groups",
            ImmutableMap.of("filter", "displayName Eq \"" + groupName1 + "\" or foo Eq \"bar\""))
        .statusCode(200)
        .body(
            "totalResults", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "Resources[0].displayName", is(groupName1),
            "Resources[0].id", is(groupId1),
            "Resources[0].meta.resourceType", is("Group"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Groups/" + groupId1));

    get("/Groups", ImmutableMap.of("filter", "displayName Eq \"notExists\""))
        .statusCode(200)
        .body("totalResults", is(0));

    delete("/Groups/" + groupId2);

    get("/Groups", ImmutableMap.of("filter", "displayName Eq \"" + groupName2 + "\""))
        .statusCode(200)
        .body("totalResults", is(0));
  }

  @Test
  public void testQueryUnsupportedFilter() {
    get("/Users", ImmutableMap.of("filter", "externalId Eq \"any\""))
        .statusCode(400)
        .body("detail", is("Unparsable filter: externalId EQ \"any\""));

    get(
            "/Users",
            ImmutableMap.of("filter", "displayName Eq \"" + groupName1 + "\" and foo Eq \"bar\""))
        .statusCode(400)
        .body("detail", is("Unparsable filter: displayName EQ \"group1\" AND foo EQ \"bar\""));
  }

  @Test
  public void testPagination() {
    post(
        "/Groups",
        String.format(
            """
                    {
                      "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
                      "displayName": "%s"
                    }
                    """,
            groupName1));

    post(
        "/Groups",
        String.format(
            """
                    {
                      "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
                      "displayName": "%s"
                    }
                    """,
            groupName2));

    get("/Groups", ImmutableMap.of("startIndex", "1", "count", "10"))
        .statusCode(200)
        .body(
            "totalResults", is(2),
            "startIndex", is(1),
            "itemsPerPage", is(2),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[1].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "Resources[1].displayName", is(groupName1),
            "Resources[1].id", is(groupId1),
            "Resources[1].meta.resourceType", is("Group"),
            "Resources[1].meta.location", endsWith("openapi/scim/v2/Groups/" + groupId1),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "Resources[0].displayName", is(groupName2),
            "Resources[0].id", is(groupId2),
            "Resources[0].meta.resourceType", is("Group"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Groups/" + groupId2));

    // the reason for order change here is TBD; since impact may not be felt in practice, not
    // exploring further at this point.

    get("/Groups", ImmutableMap.of("startIndex", "2", "count", "10"))
        .statusCode(200)
        .body(
            "totalResults", is(2),
            "startIndex", is(2),
            "itemsPerPage", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "Resources[0].displayName", is(groupName2),
            "Resources[0].id", is(groupId2),
            "Resources[0].meta.resourceType", is("Group"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Groups/" + groupId2));

    get("/Groups", ImmutableMap.of("startIndex", "1", "count", "1"))
        .statusCode(200)
        .body(
            "totalResults", is(2),
            "startIndex", is(1),
            "itemsPerPage", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "Resources[0].displayName", is(groupName1),
            "Resources[0].id", is(groupId1),
            "Resources[0].meta.resourceType", is("Group"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Groups/" + groupId1));

    get("/Groups", ImmutableMap.of("startIndex", "3", "count", "10"))
        .statusCode(200)
        .body("totalResults", is(0));

    delete("/Groups/" + groupId2);

    get("/Groups", ImmutableMap.of("startIndex", "1", "count", "10"))
        .statusCode(200)
        .body(
            "totalResults", is(1),
            "startIndex", is(1),
            "itemsPerPage", is(1),
            "schemas", contains("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
            "Resources[0].schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "Resources[0].displayName", is(groupName1),
            "Resources[0].id", is(groupId1),
            "Resources[0].meta.resourceType", is("Group"),
            "Resources[0].meta.location", endsWith("openapi/scim/v2/Groups/" + groupId1));

    get("/Groups", ImmutableMap.of("startIndex", "2", "count", "10"))
        .statusCode(200)
        .body("totalResults", is(0));
  }

  @Test
  public void testExternalId() throws Exception {
    post(
            "/Groups",
            String.format(
                """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s",
              "externalId": "someExternalId1"
            }
            """,
                groupName1))
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "displayName", is(groupName1),
            "id", is(groupId1),
            "externalId", is("someExternalId1"));

    get("/Groups/" + groupId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "displayName", is(groupName1),
            "id", is(groupId1),
            "externalId", is("someExternalId1"));

    patch(
            "/Groups/" + groupId1,
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
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "displayName", is(groupName1),
            "id", is(groupId1),
            "externalId", is("someExternalId2"));

    get("/Groups/" + groupId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "displayName", is(groupName1),
            "id", is(groupId1),
            "externalId", is("someExternalId2"));

    put(
            "/Groups/" + groupId1,
            String.format(
                """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s",
              "externalId": "someExternalId3"
            }
            """,
                groupName1))
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "displayName", is(groupName1),
            "id", is(groupId1),
            "externalId", is("someExternalId3"));

    get("/Groups/" + groupId1)
        .statusCode(200)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "displayName", is(groupName1),
            "id", is(groupId1),
            "externalId", is("someExternalId3"));
  }
}
