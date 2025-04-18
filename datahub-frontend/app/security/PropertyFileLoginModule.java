package security;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyFileLoginModule implements LoginModule {
  private static final Logger log = LoggerFactory.getLogger(PropertyFileLoginModule.class);

  private Subject subject;
  private CallbackHandler callbackHandler;
  private boolean debug = false;
  private String file;
  private boolean succeeded = false;
  private String username;

  @Override
  public void initialize(
      Subject subject,
      CallbackHandler callbackHandler,
      Map<String, ?> sharedState,
      Map<String, ?> options) {
    this.subject = subject;
    this.callbackHandler = callbackHandler;

    // Get configuration options
    this.debug = "true".equalsIgnoreCase((String) options.get("debug"));
    this.file = (String) options.get("file");

    if (debug) {
      log.debug("PropertyFileLoginModule initialized with file: {}", file);
    }
  }

  @Override
  public boolean login() throws LoginException {
    // If no file specified, this module can't authenticate
    if (file == null) {
      if (debug) log.debug("No property file specified");
      return false;
    }

    // Get username and password from callbacks
    NameCallback nameCallback = new NameCallback("Username: ");
    PasswordCallback passwordCallback = new PasswordCallback("Password: ", false);

    try {
      callbackHandler.handle(new Callback[] {nameCallback, passwordCallback});
    } catch (IOException | UnsupportedCallbackException e) {
      if (debug) log.debug("Error getting callbacks", e);
      throw new LoginException("Error during callback handling: " + e.getMessage());
    }

    this.username = nameCallback.getName();
    char[] password = passwordCallback.getPassword();
    passwordCallback.clearPassword();

    if (username == null || username.isEmpty() || password == null) {
      if (debug) log.debug("Username or password is empty");
      return false;
    }

    // Load properties file
    Properties props = new Properties();
    File propsFile = new File(file);

    if (!propsFile.exists() || !propsFile.isFile() || !propsFile.canRead()) {
      if (debug) log.debug("Cannot read property file: {}", file);
      return false;
    }

    try (FileInputStream fis = new FileInputStream(propsFile)) {
      props.load(fis);
    } catch (IOException e) {
      if (debug) log.debug("Failed to load property file", e);
      throw new LoginException("Error loading property file: " + e.getMessage());
    }

    // Check if username exists and password matches
    String storedPassword = props.getProperty(username);
    if (storedPassword == null) {
      if (debug) log.debug("User not found: {}", username);
      return false;
    }

    // Compare passwords
    succeeded = storedPassword.equals(new String(password));

    if (debug) {
      if (succeeded) {
        log.debug("Authentication succeeded for user: {}", username);
      } else {
        log.debug("Authentication failed for user: {}", username);
      }
    }

    return succeeded;
  }

  @Override
  public boolean commit() throws LoginException {
    if (!succeeded) {
      return false;
    }

    // Add principal to the subject if authentication succeeded
    subject.getPrincipals().add(new DataHubUserPrincipal(username));

    return true;
  }

  @Override
  public boolean abort() throws LoginException {
    succeeded = false;
    username = null;
    return true;
  }

  @Override
  public boolean logout() throws LoginException {
    // Remove principals that were added by this module
    subject.getPrincipals().removeIf(p -> p instanceof DataHubUserPrincipal);
    succeeded = false;
    username = null;
    return true;
  }
}
