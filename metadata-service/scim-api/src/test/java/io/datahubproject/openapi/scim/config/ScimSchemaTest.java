package io.datahubproject.openapi.scim.config;

import static org.hamcrest.Matchers.*;

import io.datahubproject.openapi.scim.repositories.ScimRepositoryTestBase;
import org.apache.directory.scim.compliance.junit.EmbeddedServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EmbeddedServerExtension.class)
public class ScimSchemaTest extends ScimRepositoryTestBase {
  @Test
  public void testGet() {
    // verifying attribute that we have updated as per our Spring configuration
    get("/Schemas")
        .statusCode(200)
        .body(
            "Resources[0].meta.location",
            endsWith("/openapi/scim/v2/Schemas/urn:ietf:params:scim:schemas:core:2.0:Group"),
            "Resources[1].meta.location",
            endsWith("/openapi/scim/v2/Schemas/urn:ietf:params:scim:schemas:core:2.0:User"));

    get("/Schemas/urn:ietf:params:scim:schemas:core:2.0:User")
        .statusCode(200)
        .body(
            "id", is("urn:ietf:params:scim:schemas:core:2.0:User"),
            "name", is("User"),
            "description", is("Top level ScimUser"),
            "meta.location",
                endsWith("/openapi/scim/v2/Schemas/urn:ietf:params:scim:schemas:core:2.0:User"));

    get("/Schemas/urn:ietf:params:scim:schemas:core:2.0:Group")
        .statusCode(200)
        .body(
            "id", is("urn:ietf:params:scim:schemas:core:2.0:Group"),
            "name", is("Group"),
            "description", is("Top level ScimGroup"),
            "meta.location",
                endsWith("/openapi/scim/v2/Schemas/urn:ietf:params:scim:schemas:core:2.0:Group"));
  }
}
