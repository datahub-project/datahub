package io.datahubproject.openapi.v1.registry;

import static org.testng.Assert.assertEquals;

import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import java.io.IOException;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.mock.env.MockEnvironment;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RegistryControllerConfigurationTest {
  @DataProvider
  public Object[][] authenticatedReadSettings() {
    return new Object[][] {{null, true}, {"true", true}, {"false", false}};
  }

  @Test(dataProvider = "authenticatedReadSettings")
  public void testAuthenticatedReadSettingBindsFromApplicationYaml(
      String environmentValue, boolean expectedEnabled) throws IOException {
    MockEnvironment environment = new MockEnvironment();
    environment
        .getPropertySources()
        .addLast(
            new YamlPropertySourceFactory()
                .createPropertySource(
                    "application", new EncodedResource(new ClassPathResource("application.yaml"))));
    if (environmentValue != null) {
      environment.setProperty("REGISTRY_API_AUTHENTICATED_READ_ENABLED", environmentValue);
    }

    AuthorizationConfiguration authorization =
        Binder.get(environment).bind("authorization", AuthorizationConfiguration.class).get();

    assertEquals(authorization.getRegistryApi().isAuthenticatedReadEnabled(), expectedEnabled);
  }
}
