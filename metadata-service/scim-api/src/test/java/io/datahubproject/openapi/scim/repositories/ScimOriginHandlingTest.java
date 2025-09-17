package io.datahubproject.openapi.scim.repositories;

import static org.hamcrest.Matchers.*;

import io.restassured.response.ValidatableResponse;
import java.util.Base64;
import org.apache.directory.scim.compliance.junit.EmbeddedServerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EmbeddedServerExtension.class)
public class ScimOriginHandlingTest extends ScimRepositoryTestBase {
  String groupName1 = "originTestGroup1";
  String groupId1 =
      Base64.getUrlEncoder().encodeToString(("urn:li:corpGroup:" + groupName1).getBytes());

  String userName1 = "originTestUser1@example.com";
  String userId1 =
      Base64.getUrlEncoder().encodeToString(("urn:li:corpuser:" + userName1).getBytes());

  @AfterEach
  public void cleanup() {
    delete("/Users/" + userId1);
    delete("/Groups/" + groupId1);
  }

  @Test
  public void testCreateGroupWithNullExternalId() throws Exception {
    // Test creating a group without externalId - should handle null gracefully
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
            "externalId", nullValue());

    // Verify GET also returns null externalId
    get("/Groups/" + groupId1)
        .statusCode(200)
        .body(
            "displayName", is(groupName1),
            "id", is(groupId1),
            "externalId", nullValue());
  }

  @Test
  public void testCreateUserWithNullExternalId() throws Exception {
    // Test creating a user without externalId - should handle null gracefully
    String createBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s"
            }
            """,
            userName1);

    ValidatableResponse validatableResponse = post("/Users", createBody);

    validatableResponse
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "externalId", nullValue());

    // Verify GET also returns null externalId
    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "userName", is(userName1),
            "id", is(userId1),
            "externalId", nullValue());
  }

  @Test
  public void testCreateGroupWithExternalId() throws Exception {
    // Test creating a group with externalId - should store and retrieve properly
    String externalId = "external-group-123";
    String createBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s",
              "externalId": "%s"
            }
            """,
            groupName1, externalId);

    ValidatableResponse validatableResponse = post("/Groups", createBody);

    validatableResponse
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "displayName", is(groupName1),
            "id", is(groupId1),
            "externalId", is(externalId));

    // Verify GET also returns the externalId
    get("/Groups/" + groupId1)
        .statusCode(200)
        .body(
            "displayName", is(groupName1),
            "id", is(groupId1),
            "externalId", is(externalId));
  }

  @Test
  public void testCreateUserWithExternalId() throws Exception {
    // Test creating a user with externalId - should store and retrieve properly
    String externalId = "external-user-456";
    String createBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s",
              "externalId": "%s"
            }
            """,
            userName1, externalId);

    ValidatableResponse validatableResponse = post("/Users", createBody);

    validatableResponse
        .statusCode(201)
        .body(
            "schemas", contains("urn:ietf:params:scim:schemas:core:2.0:User"),
            "userName", is(userName1),
            "id", is(userId1),
            "externalId", is(externalId));

    // Verify GET also returns the externalId
    get("/Users/" + userId1)
        .statusCode(200)
        .body(
            "userName", is(userName1),
            "id", is(userId1),
            "externalId", is(externalId));
  }

  @Test
  public void testUpdateExternalIdFromNullToValue() throws Exception {
    // Test updating externalId from null to a value
    String createBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s"
            }
            """,
            groupName1);

    post("/Groups", createBody).statusCode(201).body("externalId", nullValue());

    String externalId = "updated-external-id";
    patch(
            "/Groups/" + groupId1,
            String.format(
                """
            {
              "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
              "Operations": [
                {
                "op":"add",
                "path":"externalId",
                "value":"%s"
              }]
            }
            """,
                externalId))
        .statusCode(200)
        .body("externalId", is(externalId));

    // Verify persistence
    get("/Groups/" + groupId1).statusCode(200).body("externalId", is(externalId));
  }

  @Test
  public void testUpdateExternalIdFromValueToNull() throws Exception {
    // Test updating externalId from a value to null (remove operation)
    String externalId = "temp-external-id";
    String createBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
              "displayName": "%s",
              "externalId": "%s"
            }
            """,
            groupName1, externalId);

    post("/Groups", createBody).statusCode(201).body("externalId", is(externalId));

    patch(
            "/Groups/" + groupId1,
            """
            {
              "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
              "Operations": [
                {
                "op":"remove",
                "path":"externalId"
              }]
            }
            """)
        .statusCode(200)
        .body("externalId", nullValue());

    // Verify persistence
    get("/Groups/" + groupId1).statusCode(200).body("externalId", nullValue());
  }

  @Test
  public void testReplaceExternalId() throws Exception {
    // Test replacing externalId with a different value
    String initialExternalId = "initial-external-id";
    String createBody =
        String.format(
            """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s",
              "externalId": "%s"
            }
            """,
            userName1, initialExternalId);

    post("/Users", createBody).statusCode(201).body("externalId", is(initialExternalId));

    String newExternalId = "new-external-id";
    put(
            "/Users/" + userId1,
            String.format(
                """
            {
              "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
              "userName": "%s",
              "externalId": "%s"
            }
            """,
                userName1, newExternalId))
        .statusCode(200)
        .body("externalId", is(newExternalId));

    // Verify persistence
    get("/Users/" + userId1).statusCode(200).body("externalId", is(newExternalId));
  }
}
