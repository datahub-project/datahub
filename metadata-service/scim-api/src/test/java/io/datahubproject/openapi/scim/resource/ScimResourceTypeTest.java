package io.datahubproject.openapi.scim.resource;

import static org.hamcrest.Matchers.*;

import io.datahubproject.openapi.scim.repositories.ScimRepositoryTestBase;
import org.apache.directory.scim.compliance.junit.EmbeddedServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EmbeddedServerExtension.class)
public class ScimResourceTypeTest extends ScimRepositoryTestBase {

  @Test
  public void testGet() {
    get("/ResourceTypes/User")
        .statusCode(200)
        .body(
            "id", is("User"),
            "meta.location", endsWith("/openapi/scim/v2/ResourceTypes/User"));

    get("/ResourceTypes/Group")
        .statusCode(200)
        .body(
            "id", is("Group"),
            "meta.location", endsWith("/openapi/scim/v2/ResourceTypes/Group"));

    get("/ResourceTypes")
        .statusCode(200)
        .body(
            "totalResults",
            is(2),
            "Resources[0].meta.location",
            endsWith("/openapi/scim/v2/ResourceTypes/Group"),
            "Resources[1].meta.location",
            endsWith("/openapi/scim/v2/ResourceTypes/User"));
  }
}
