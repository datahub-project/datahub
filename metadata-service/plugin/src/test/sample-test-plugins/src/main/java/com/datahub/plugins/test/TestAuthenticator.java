package com.datahub.plugins.test;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.plugins.PluginConstant;
import com.datahub.plugins.auth.authentication.Authenticator;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessControlException;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestAuthenticator implements Authenticator {
  private AuthenticatorContext _authenticatorContext;

  @Override
  public void init(
      @Nonnull Map<String, Object> authenticatorConfig, @Nullable AuthenticatorContext context) {
    /*
     * authenticatorConfig contains key, value pairs set in plugins[].params.configs of config.yml
     */
    this._authenticatorContext = context;
    assert authenticatorConfig.containsKey("key1");
    assert authenticatorConfig.containsKey("key2");
    assert authenticatorConfig.containsKey("key3");
    assert authenticatorConfig.get("key1").equals("value1");
    assert authenticatorConfig.get("key2").equals("value2");
    assert authenticatorConfig.get("key3").equals("value3");

    log.info("Init succeed");
  }

  private void readInputStream() {
    // Test resource as stream is working
    try (InputStream inputStream =
        this.getClass().getClassLoader().getResourceAsStream("foo_bar.json")) {
      assert inputStream != null;
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      assert reader.readLine() != null;
      log.info("authenticate succeed");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void accessFile() {
    // Try to create a file on PLUGIN_DIRECTORY to test plugin should have permission to read/write
    // on plugin directory
    Path pluginDirectory =
        Paths.get(
            (String) this._authenticatorContext.data().get(PluginConstant.PLUGIN_HOME),
            "tmp_file1.txt");
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
  }

  public void accessSystemProperty() {
    try {
      System.getProperty("user.home");
      throw new RuntimeException(
          "Plugin is able to access system properties"); // we should not reach here
    } catch (AccessControlException accessControlException) {
      log.info("Expected: Don't have permission to read system properties");
    }
  }

  public void accessSocket() {
    try {
      URL url = new URL("https://github.com");
      try (InputStream input = url.openStream()) {
        assert input != null;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void accessLowerSocket() {
    try {
      new Socket("localhost", 50);
      throw new RuntimeException("Plugin is able to access lower port");
    } catch (AccessControlException e) {
      log.info("Expected: Don't have permission to open socket on lower port");
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  @Override
  public Authentication authenticate(@Nonnull AuthenticationRequest authenticationRequest)
      throws AuthenticationException {
    // Call some resource related API to test IsolatedClassLoader
    URL url = this.getClass().getClassLoader().getResource("foo_bar.json");
    assert url != null;
    // Test IsolatedClassLoader stream access
    this.readInputStream();
    // We should have permission to write and delete file from plugin directory
    this.accessFile();
    // We should not have access to System properties
    this.accessSystemProperty();
    // We should be able to open socket
    this.accessSocket();
    // We should not be able to access lower socket
    this.accessLowerSocket();

    return new Authentication(new Actor(ActorType.USER, "fake"), "foo:bar");
  }
}
