package io.datahubproject.openapi.scim.resource;

import static org.hamcrest.Matchers.*;

import io.datahubproject.openapi.scim.repositories.ScimRepositoryTestBase;
import org.apache.directory.scim.compliance.junit.EmbeddedServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EmbeddedServerExtension.class)
public class ScimResourceSearch extends ScimRepositoryTestBase {
  @Test
  public void testUserSearch() {
    String createUserPayload =
        """
       {
          "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
          "userName": "testUser1",
          "externalId": "ghjk"
       }
        """;

    String searchPayload =
        """
        {
        	"schemas": [
        		"urn:ietf:params:scim:api:messages:2.0:SearchRequest"
        	],
        	"attributes": [
        		"userName", "active"
        	],
        	"filter": "userName eq \\"testUser1\\"",
        	"startIndex": 1,
        	"count": 10
        }
        """;

    post("/Users", createUserPayload).statusCode(201);

    post("/Users/.search", searchPayload)
        .statusCode(200)
        .body("totalResults", is(1), "Resources[0].userName", is("testUser1"));
  }
}
