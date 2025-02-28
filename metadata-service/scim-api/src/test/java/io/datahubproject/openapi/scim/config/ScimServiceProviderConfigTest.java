package io.datahubproject.openapi.scim.config;

import static org.hamcrest.Matchers.*;

import io.datahubproject.openapi.scim.repositories.ScimRepositoryTestBase;
import org.apache.directory.scim.compliance.junit.EmbeddedServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EmbeddedServerExtension.class)
public class ScimServiceProviderConfigTest extends ScimRepositoryTestBase {
  @Test
  public void testBasicCRUD() throws Exception {
    get("/ServiceProviderConfig")
        .statusCode(200)
        .body(
            "authenticationSchemes[0].type",
            is("oauthbearertoken"),
            "filter.supported",
            is(true),
            "meta.location",
            endsWith("/openapi/scim/v2/ServiceProviderConfig"));
  }
}
