/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dao.FlowsDAO;
import dao.MetricsDAO;
import dao.UserDAO;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import javax.naming.AuthenticationException;
import javax.naming.NamingException;
import javax.persistence.EntityManagerFactory;
import org.apache.commons.lang3.StringUtils;
import play.data.DynamicForm;
import play.libs.Json;
import play.Logger;
import play.Play;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Results;
import play.mvc.Security;
import security.AuthenticationManager;
import utils.Tree;
import wherehows.dao.ConnectionPoolProperties;
import wherehows.dao.DaoFactory;

import static play.data.Form.*;


public class Application extends Controller {

  private static final String TREE_NAME_SUBFIX = ".tree.name";

  private static final String WHZ_APP_ENV = System.getenv("WHZ_APP_HOME");
  private static final String APP_VERSION = Play.application().configuration().getString("app.version");
  private static final String PIWIK_SITE_ID = Play.application().configuration().getString("tracking.piwik.siteid");
  private static final String PIWIK_URL = Play.application().configuration().getString("tracking.piwik.url");
  private static final Boolean IS_INTERNAL = Play.application().configuration().getBoolean("linkedin.internal", false);
  private static final String JIT_ACL_WHITELIST =
      Play.application().configuration().getString("linkedin.jit.acl.whitelist", "");
  private static final String WHZ_DS_OWNERSHIP_TAB =
      Play.application().configuration().getString("ui.show.ownership.revamp", "hide");
  private static final Boolean WHZ_STG_BANNER =
      Play.application().configuration().getBoolean("ui.show.staging.banner", false);
  private static final Boolean WHZ_STALE_SEARCH_ALERT =
      Play.application().configuration().getBoolean("ui.show.stale.search", false);
  private static final Boolean HTTPS_REDIRECT = Play.application().configuration().getBoolean("https.redirect", false);
  private static final Boolean WHZ_SHOW_LINEAGE =
      Play.application().configuration().getBoolean("linkedin.show.dataset.lineage", false);
  private static final Boolean WHZ_SHOW_DS_HEALTH =
      Play.application().configuration().getBoolean("linkedin.show.dataset.health", false);
  private static final Boolean WHZ_SUGGESTION_CONFIDENCE_THRESHOLD =
      Play.application().configuration().getString("linkedin.suggestion.confidence.threshold", "50");

  private static final String WHZ_WIKI_LINKS__GDRP_PII =
      Play.application().configuration().getString("linkedin.links.wiki.gdprPii", "");
  private static final String WHZ_WIKI_LINKS__TMS_SCHEMA =
      Play.application().configuration().getString("linkedin.links.wiki.tmsSchema", "");
  private static final String WHZ_WIKI_LINKS__GDPR_TAXONOMY =
      Play.application().configuration().getString("linkedin.links.wiki.gdprTaxonomy", "");
  private static final String WHZ_WIKI_LINKS__STALE_SEARCH_INDEX =
      Play.application().configuration().getString("linkedin.links.wiki.staleSearchIndex", "");
  private static final String WHZ_WIKI_LINKS__DHT =
      Play.application().configuration().getString("linkedin.links.wiki.dht", "");
  private static final String WHZ_WIKI_LINKS__PURGE_POLICIES =
      Play.application().configuration().getString("linkedin.links.wiki.purgePolicies", "");
  private static final String WHZ_WIKI_LINKS__JIT_ACL_FAQ =
      Play.application().configuration().getString("linkedin.links.wiki.jitAcl", "");
  private static final String WHZ_WIKI_LINKS__METADATA_CUSTOM_REGEX =
      Play.application().configuration().getString("linkedin.links.wiki.metadataCustomRegex", "");
  private static final String WHZ_WIKI_LINKS__COMPLIANCE_OWNER =
      Play.application().configuration().getString("linkedin.links.wiki.complianceOwner", "");

  private static final String DB_WHEREHOWS_URL =
      Play.application().configuration().getString("database.opensource.url");
  private static final String WHZ_DB_DSCLASSNAME =
      Play.application().configuration().getString("hikaricp.dataSourceClassName");
  private static final String DB_WHEREHOWS_USERNAME =
      Play.application().configuration().getString("database.opensource.username");
  private static final String DB_WHEREHOWS_PASSWORD =
      Play.application().configuration().getString("database.opensource.password");
  private static final String DB_WHEREHOWS_DIALECT = Play.application().configuration().getString("hikaricp.dialect");
  private static final String DAO_FACTORY_CLASS =
      Play.application().configuration().getString("dao.factory.class", DaoFactory.class.getCanonicalName());

  private static final EntityManagerFactory ENTITY_MANAGER_FACTORY = ConnectionPoolProperties.builder()
      .dataSourceClassName(WHZ_DB_DSCLASSNAME)
      .dataSourceURL(DB_WHEREHOWS_URL)
      .dataSourceUser(DB_WHEREHOWS_USERNAME)
      .dataSourcePassword(DB_WHEREHOWS_PASSWORD)
      .dialect(DB_WHEREHOWS_DIALECT)
      .build()
      .buildEntityManagerFactory();

  public static final DaoFactory DAO_FACTORY = createDaoFactory();

  private static DaoFactory createDaoFactory() {
    try {
      Logger.info("Creating DAO factory: " + DAO_FACTORY_CLASS);
      Class factoryClass = Class.forName(DAO_FACTORY_CLASS);
      Constructor<? extends DaoFactory> ctor = factoryClass.getConstructor(EntityManagerFactory.class);
      return ctor.newInstance(ENTITY_MANAGER_FACTORY);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Serves the build output index.html for any given path
   *
   * @param path takes a path string, which essentially is ignored
   *             routing is managed client side
   * @return {Result} build output index.html resource
   */
  private static Result serveAsset(String path) {
    Result redirect = redirectToHttpsIfNeeded();
    if (redirect != null) {
      return redirect;
    }

    InputStream indexHtml = Play.application().classloader().getResourceAsStream("public/index.html");
    response().setHeader("Cache-Control", "no-cache");

    return ok(indexHtml).as("text/html");
  }

  public static Result healthcheck() {
    return ok("GOOD");
  }

  public static Result printDeps() {
    String libPath = WHZ_APP_ENV + "/lib";
    String commitFile = WHZ_APP_ENV + "/commit";
    String commit = "";

    if (WHZ_APP_ENV == null) {
      return ok("WHZ_APP_HOME environmental variable not defined");
    }

    try {
      commit = new BufferedReader(new FileReader(commitFile)).readLine();
    } catch (IOException ioe) {
      Logger.error("Error while reading commit file. Error message: " + ioe.getMessage());
    }

    //get all the files from /libs directory
    File directory = new File(libPath);
    StringBuilder sb = new StringBuilder();
    if (directory.listFiles() != null) {
      for (File file : directory.listFiles()) {
        if (file.isFile()) {
          sb.append(file.getName()).append("\n");
        }
      }
    }

    return ok("commit: " + commit + "\n" + "libraries: " + sb.toString());
  }

  /**
   * index Action proxies to serveAsset
   *
   * @param path takes a path string which is either index.html or the path segment after /
   * @return {Result} response from serveAsset method
   */
  public static Result index(String path) {
    Result redirect = redirectToHttpsIfNeeded();
    if (redirect != null) {
      return redirect;
    }

    return serveAsset("");
  }

  /**
   * Creates a wrapping ObjectNode containing config information
   *
   * @return Http Result instance with app configuration attributes
   */
  public static Result appConfig() {
    ObjectNode response = Json.newObject();
    ObjectNode config = Json.newObject();

    config.put("appVersion", APP_VERSION);
    config.put("isInternal", IS_INTERNAL);
    config.put("shouldShowDatasetLineage", WHZ_SHOW_LINEAGE);
    config.put("shouldShowDatasetHealth", WHZ_SHOW_DS_HEALTH);
    config.put("suggestionConfidenceThreshold", Integer.parseInt(WHZ_SUGGESTION_CONFIDENCE_THRESHOLD));
    config.set("wikiLinks", wikiLinks());
    config.set("JitAclAccessWhitelist", Json.toJson(StringUtils.split(JIT_ACL_WHITELIST, ',')));
    config.set("tracking", trackingInfo());
    // In a staging environment, we can trigger this flag to be true so that the UI can handle based on
    // such config and alert users that their changes will not affect production data
    config.put("isStagingBanner", WHZ_STG_BANNER);
    // Flag set in order to warn users that search is experiencing issues
    config.put("isStaleSearch", WHZ_STALE_SEARCH_ALERT);
    response.put("status", "ok");
    response.set("config", config);

    return ok(response);
  }

  /**
   * @return Json object with internal wiki links
   */
  private static ObjectNode wikiLinks() {
    ObjectNode wikiLinks = Json.newObject();

    wikiLinks.put("gdprPii", WHZ_WIKI_LINKS__GDRP_PII);
    wikiLinks.put("tmsSchema", WHZ_WIKI_LINKS__TMS_SCHEMA);
    wikiLinks.put("gdprTaxonomy", WHZ_WIKI_LINKS__GDPR_TAXONOMY);
    wikiLinks.put("staleSearchIndex", WHZ_WIKI_LINKS__STALE_SEARCH_INDEX);
    wikiLinks.put("dht", WHZ_WIKI_LINKS__DHT);
    wikiLinks.put("purgePolicies", WHZ_WIKI_LINKS__PURGE_POLICIES);
    wikiLinks.put("jitAcl", WHZ_WIKI_LINKS__JIT_ACL_FAQ);
    wikiLinks.put("metadataCustomRegex", WHZ_WIKI_LINKS__METADATA_CUSTOM_REGEX);

    return wikiLinks;
  }

  private static Result redirectToHttpsIfNeeded() {
    Request request = request();
    if (!HTTPS_REDIRECT || request.secure()) {
      return null;
    }

    String url = "https://" + request.host() + request.uri();
    Logger.info("Redirecting to " + url);
    return redirect(url);
  }

  /**
   * @return Json object containing the tracking configuration details
   */
  private static ObjectNode trackingInfo() {
    ObjectNode trackingConfig = Json.newObject();
    ObjectNode trackers = Json.newObject();
    ObjectNode piwik = Json.newObject();

    Integer siteId = null;
    try {
      siteId = Integer.parseInt(PIWIK_SITE_ID);
    } catch (NumberFormatException e) {
      Logger.error("Piwik site ID must be an integer");
    }

    piwik.put("piwikSiteId", siteId);
    piwik.put("piwikUrl", PIWIK_URL);
    trackers.set("piwik", piwik);
    trackingConfig.set("trackers", trackers);
    trackingConfig.put("isEnabled", true);

    return trackingConfig;
  }

  @Security.Authenticated(Secured.class)
  public static Result lineage() {
    String username = session("user");
    if (username == null) {
      username = "";
    }
    return serveAsset("");
  }

  @Security.Authenticated(Secured.class)
  public static Result datasetLineage(int id) {
    String username = session("user");
    if (username == null) {
      username = "";
    }

    return serveAsset("");
  }

  @Security.Authenticated(Secured.class)
  public static Result metricLineage(int id) {
    String username = session("user");
    if (username == null) {
      username = "";
    }

    return serveAsset("");
  }

  @Security.Authenticated(Secured.class)
  public static Result flowLineage(String application, String project, String flow) {
    String username = session("user");
    if (username == null) {
      username = "";
    }
    String type = "azkaban";
    if (StringUtils.isNotBlank(application) && (application.toLowerCase().indexOf("appworx") != -1)) {
      type = "appworx";
    }

    return serveAsset("");
  }

  @Security.Authenticated(Secured.class)
  public static Result schemaHistory() {
    String username = session("user");
    if (username == null) {
      username = "";
    }

    return serveAsset("");
  }

  @Security.Authenticated(Secured.class)
  public static Result scriptFinder() {
    String username = session("user");
    if (username == null) {
      username = "";
    }

    return serveAsset("");
  }

  @Security.Authenticated(Secured.class)
  public static Result idpc() {
    String username = session("user");
    if (username == null) {
      username = "";
    }
    return serveAsset("");
  }

  @Security.Authenticated(Secured.class)
  public static Result dashboard() {
    String username = session("user");
    if (username == null) {
      username = "";
    }
    return serveAsset("");
  }

  public static Result login() {
    //You cann generate the Csrf token such as String csrfToken = SecurityPlugin.getInstance().getCsrfToken();
    String csrfToken = "";
    return serveAsset("");
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result authenticate() throws NamingException {
    JsonNode json = request().body().asJson();
    // Extract username and password as String from JsonNode,
    //   null if they are not strings
    String username = json.findPath("username").textValue();
    String password = json.findPath("password").textValue();

    if (username == null || StringUtils.isBlank(username)) {
      return badRequest("Missing or invalid [username]");
    }
    if (password == null || StringUtils.isBlank(password)) {
      return badRequest("Missing or invalid [credentials]");
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

  public static Result signUp() {
    DynamicForm loginForm = form().bindFromRequest();
    String username = loginForm.get("inputName");
    String firstName = loginForm.get("inputFirstName");
    String lastName = loginForm.get("inputLastName");
    String email = loginForm.get("inputEmail");
    String password = loginForm.get("inputPassword");
    String errorMessage = "";
    try {
      errorMessage = UserDAO.signUp(username, firstName, lastName, email, password);
      if (StringUtils.isNotBlank(errorMessage)) {
        flash("error", errorMessage);
      } else {
        flash("success", "Congratulations! Your account has been created. Please login.");
      }
    } catch (Exception e) {
      flash("error", e.getMessage());
    }

    return serveAsset("");
  }

  public static Result logout() {
    session().clear();
    return ok();
  }

  public static Result loadTree(String key) {
    if (StringUtils.isNotBlank(key) && key.equalsIgnoreCase("flows")) {
      return ok(FlowsDAO.getFlowApplicationNodes());
    } else if (StringUtils.isNotBlank(key) && key.equalsIgnoreCase("metrics")) {
      return ok(MetricsDAO.getMetricDashboardNodes());
    }
    return ok(Tree.loadTreeJsonNode(key + TREE_NAME_SUBFIX));
  }

  public static Result loadFlowProjects(String app) {
    return ok(FlowsDAO.getFlowProjectNodes(app));
  }

  public static Result loadFlowNodes(String app, String project) {
    return ok(FlowsDAO.getFlowNodes(app, project));
  }

  public static Result loadMetricGroups(String dashboard) {
    return ok(MetricsDAO.getMetricGroupNodes(dashboard));
  }

  public static Result loadMetricNodes(String dashboard, String group) {
    return ok(MetricsDAO.getMetricNodes(dashboard, group));
  }
}
