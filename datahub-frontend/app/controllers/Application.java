package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import play.Logger;
import play.Play;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;
import security.AuthUtil;
import security.AuthenticationManager;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.naming.AuthenticationException;
import javax.naming.NamingException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.stream.Stream;


public class Application extends Controller {

  private final Config _config;

  @Inject
  public Application(@Nonnull Config config) {
    _config = config;
  }

  /**
   * Serves the build output index.html for any given path
   *
   * @param path takes a path string, which essentially is ignored
   *             routing is managed client side
   * @return {Result} build output index.html resource
   */
  @Nonnull
  private Result serveAsset(@Nullable String path) {
    InputStream indexHtml = Play.application().classloader().getResourceAsStream("public/index.html");
    response().setHeader("Cache-Control", "no-cache");

    return ok(indexHtml).as("text/html");
  }

  @Nonnull
  public Result healthcheck() {
    return ok("GOOD");
  }

  @Nonnull
  public Result printDeps() {
    final String appHome = System.getenv("WHZ_APP_HOME");

    String libPath = appHome + "/lib";
    String commitFile = appHome + "/commit";
    String commit = "";

    if (appHome == null) {
      return ok("WHZ_APP_HOME environmental variable not defined");
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(commitFile))) {
      commit = reader.readLine();
    } catch (IOException ioe) {
      Logger.error("Error while reading commit file. Error message: " + ioe.getMessage());
    }

    //get all the files from /libs directory
    StringBuilder sb = new StringBuilder();

    try (Stream<Path> paths = Files.list(Paths.get(libPath))) {
      paths.filter(Files::isRegularFile).
          forEach(f -> sb.append(f.getFileName()).append("\n"));
    } catch (IOException ioe) {
      Logger.error("Error while traversing the directory. Error message: " + ioe.getMessage());
    }

    return ok("commit: " + commit + "\n" + "libraries: " + sb.toString());
  }

  /**
   * index Action proxies to serveAsset
   *
   * @param path takes a path string which is either index.html or the path segment after /
   * @return {Result} response from serveAsset method
   */
  @Nonnull
  public Result index(@Nullable String path) {
    return serveAsset("");
  }

  /**
   * Generic not found response
   * @param path
   * @return
   */
  @Nonnull
  public Result apiNotFound(@Nullable String path) {
    return badRequest("{\"error\": \"API endpoint does not exist\"}");
  }

  /**
   * Creates a wrapping ObjectNode containing config information
   *
   * @return Http Result instance with app configuration attributes
   */
  @Nonnull
  public Result appConfig() {
    final ObjectNode config = Json.newObject();
    config.put("appVersion", _config.getString("app.version"));
    config.put("isInternal", _config.getBoolean("linkedin.internal"));
    config.put("shouldShowDatasetLineage", _config.getBoolean("linkedin.show.dataset.lineage"));
    config.put("suggestionConfidenceThreshold",
        Integer.valueOf(_config.getString("linkedin.suggestion.confidence.threshold")));
    config.set("wikiLinks", wikiLinks());
    config.set("tracking", trackingInfo());
    // In a staging environment, we can trigger this flag to be true so that the UI can handle based on
    // such config and alert users that their changes will not affect production data
    config.put("isStagingBanner", _config.getBoolean("ui.show.staging.banner"));
    config.put("isLiveDataWarning", _config.getBoolean("ui.show.live.data.banner"));
    config.put("showChangeManagement", _config.getBoolean("ui.show.CM.banner"));
    // Flag to enable people entity elements
    config.put("showPeople", _config.getBoolean("ui.show.people"));
    config.put("changeManagementLink", _config.getString("ui.show.CM.link"));
    // Flag set in order to warn users that search is experiencing issues
    config.put("isStaleSearch", _config.getBoolean("ui.show.stale.search"));
    config.put("showAdvancedSearch", _config.getBoolean("ui.show.advanced.search"));
    // Flag to use the new api for browsing datasets
    config.put("useNewBrowseDataset", _config.getBoolean("ui.new.browse.dataset"));
    // show lineage graph in relationships tabs
    config.put("showLineageGraph", _config.getBoolean("ui.show.lineage.graph"));
    // show institutional memory for available entities
    config.put("showInstitutionalMemory", _config.getBoolean("ui.show.institutional.memory"));

    // Insert properties for user profile operations
    config.set("userEntityProps", userEntityProps());

    final ObjectNode response = Json.newObject();
    response.put("status", "ok");
    response.set("config", config);
    return ok(response);
  }

  /**
   * Creates a JSON object of profile / avatar properties
   * @return Json avatar / profile image properties
   */
  @Nonnull
  private ObjectNode userEntityProps() {
    final ObjectNode props = Json.newObject();
    props.put("aviUrlPrimary", _config.getString("linkedin.links.avi.urlPrimary"));
    props.put("aviUrlFallback", _config.getString("linkedin.links.avi.urlFallback"));
    return props;
  }

  /**
   * @return Json object with internal wiki links
   */
  @Nonnull

  private ObjectNode wikiLinks() {
    final ObjectNode wikiLinks = Json.newObject();
    wikiLinks.put("appHelp", _config.getString("links.wiki.appHelp"));
    wikiLinks.put("gdprPii", _config.getString("links.wiki.gdprPii"));
    wikiLinks.put("tmsSchema", _config.getString("links.wiki.tmsSchema"));
    wikiLinks.put("gdprTaxonomy", _config.getString("links.wiki.gdprTaxonomy"));
    wikiLinks.put("staleSearchIndex", _config.getString("links.wiki.staleSearchIndex"));
    wikiLinks.put("dht", _config.getString("links.wiki.dht"));
    wikiLinks.put("purgePolicies", _config.getString("links.wiki.purgePolicies"));
    wikiLinks.put("jitAcl", _config.getString("links.wiki.jitAcl"));
    wikiLinks.put("metadataCustomRegex", _config.getString("links.wiki.metadataCustomRegex"));
    wikiLinks.put("exportPolicy", _config.getString("links.wiki.exportPolicy"));
    wikiLinks.put("metadataHealth", _config.getString("links.wiki.metadataHealth"));
    wikiLinks.put("purgeKey", _config.getString("links.wiki.purgeKey"));
    wikiLinks.put("datasetDecommission", _config.getString("links.wiki.datasetDecommission"));
    return wikiLinks;
  }

  /**
   * @return Json object containing the tracking configuration details
   */
  @Nonnull
  private ObjectNode trackingInfo() {
    final ObjectNode piwik = Json.newObject();
    piwik.put("piwikSiteId", Integer.valueOf(_config.getString("tracking.piwik.siteid")));
    piwik.put("piwikUrl", _config.getString("tracking.piwik.url"));

    final ObjectNode trackers = Json.newObject();
    trackers.set("piwik", piwik);

    final ObjectNode trackingConfig = Json.newObject();
    trackingConfig.set("trackers", trackers);
    trackingConfig.put("isEnabled", true);
    return trackingConfig;
  }

  @Nonnull
  public Result login() {
    //You can generate the Csrf token such as String csrfToken = SecurityPlugin.getInstance().getCsrfToken();
    String csrfToken = "";
    return serveAsset("");
  }

  @BodyParser.Of(BodyParser.Json.class)
  @Nonnull
  public Result authenticate() throws NamingException {
    JsonNode json = request().body().asJson();
    // Extract username and password as String from JsonNode,
    //   null if they are not strings
    String username = json.findPath("username").textValue();
    String password = json.findPath("password").textValue();

    if (StringUtils.isBlank(username)) {
      return badRequest("Missing or invalid [username]");
    }
    if (password == null) {
      password = "";
    }

    session().clear();

    // Create a uuid string for this session if one doesn't already exist
    //   to be appended to the Result object
    String uuid = session("uuid");
    if (uuid == null) {
      uuid = java.util.UUID.randomUUID().toString();
      session("uuid", uuid);
    }

    try {
      AuthenticationManager.authenticateUser(username, password);
    } catch (AuthenticationException e) {
      Logger.warn("Authentication error!", e);
      return badRequest("Invalid Credential");
    }

    // Adds the username to the session cookie
    session("user", username);

    String secretKey = _config.getString("play.http.secret.key");
    try {
      //store hashed username to PLAY_SESSION cookie
      String hashedUserName = AuthUtil.generateHash(username, secretKey.getBytes());
      session("auth_token", hashedUserName);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      Logger.error("Failed to hash username", e);
    }

    // Construct an ObjectNode with the username and uuid token to be sent with the response
    ObjectNode data = Json.newObject();
    data.put("username", username);
    data.put("uuid", uuid);

    // Create a new response ObjectNode to return when authenticate request is successful
    ObjectNode response = Json.newObject();
    response.put("status", "ok");
    response.set("data", data);
    return ok(response);
  }

  @Nonnull
  public Result logout() {
    session().clear();
    return ok();
  }
}
