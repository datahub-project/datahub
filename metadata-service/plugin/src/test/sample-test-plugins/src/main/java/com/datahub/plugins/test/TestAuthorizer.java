package com.datahub.plugins.test;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizedActors;
import com.datahub.authorization.AuthorizerContext;
import com.datahub.authorization.ResourceSpec;
import com.datahub.plugins.PluginConstant;
import com.datahub.plugins.auth.authorization.Authorizer;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TestAuthorizer implements Authorizer {
  private AuthorizerContext _authorizerContext;

  @Override
  public void init(@Nonnull Map<String, Object> authorizerConfig, @Nonnull AuthorizerContext ctx) {
    this._authorizerContext = ctx;
    assert authorizerConfig.containsKey("key1");
    assert authorizerConfig.containsKey("key2");
    assert authorizerConfig.containsKey("key3");
    assert authorizerConfig.get("key1").equals("value1");
    assert authorizerConfig.get("key2").equals("value2");
    assert authorizerConfig.get("key3").equals("value3");

    log.info("Init succeed");
  }

  @Override
  public AuthorizationResult authorize(@Nonnull AuthorizationRequest request) {
    // Call some resource related API to test IsolatedClassLoader
    URL url = this.getClass().getClassLoader().getResource("foo_bar.json");
    assert url != null;

    // Try to create a file on PLUGIN_DIRECTORY to test plugin should have permission to read/write on plugin directory
    Path pluginDirectory =
        Paths.get((String) this._authorizerContext.data().get(PluginConstant.PLUGIN_HOME), "tmp_file1.txt");
    try {

      try (BufferedWriter writer = new BufferedWriter(new FileWriter(pluginDirectory.toString()))) {
        writer.write("Happy writing");
      }

      if (!pluginDirectory.toFile().delete()) {
        throw new IOException("Not able to delete file");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Test resource as stream is working
    try (InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("foo_bar.json")) {
      assert inputStream != null;
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      assert reader.readLine() != null;
      log.info("authorizer succeed");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "fake message");
  }

  @Override
  public AuthorizedActors authorizedActors(String privilege, Optional<ResourceSpec> resourceSpec) {
    return new AuthorizedActors("ALL", null, null, true, true);
  }
}

