package io.datahubproject.openapi.scim;

import java.net.URI;
import org.apache.directory.scim.compliance.junit.EmbeddedServerExtension;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

public class SpringScimTestServer implements EmbeddedServerExtension.ScimTestServer {

  private ConfigurableApplicationContext context;

  @Override
  public URI start(int port) {
    context =
        SpringApplication.run(
            ScimpleSpringTestApp.class, "--server.servlet.context-path=/", "--server.port=" + port);
    return URI.create("http://localhost:" + port + "/openapi/scim/v2");
  }

  @Override
  public void shutdown() {
    context.stop();
  }
}
