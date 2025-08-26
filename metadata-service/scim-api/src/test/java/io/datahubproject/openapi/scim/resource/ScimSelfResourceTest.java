package io.datahubproject.openapi.scim.resource;

import io.datahubproject.openapi.scim.repositories.ScimRepositoryTestBase;
import org.apache.directory.scim.compliance.junit.EmbeddedServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EmbeddedServerExtension.class)
public class ScimSelfResourceTest extends ScimRepositoryTestBase {
  @Test
  public void testBasicCRUD() throws Exception {

    post("/Me", "").statusCode(501);

    put("/Me", "").statusCode(501);

    patch("/Me", "").statusCode(501);

    get("/Me").statusCode(501);

    delete("/Me").statusCode(501);
  }
}
