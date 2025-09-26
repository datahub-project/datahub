package controllers;

import com.linkedin.metadata.utils.BasePathUtils;
import javax.inject.Inject;
import javax.inject.Singleton;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Result;

/**
 * Controller for serving robots.txt with proper base path configuration.
 *
 * <p>This controller generates robots.txt dynamically to include the correct base path for DataHub
 * deployments behind reverse proxies or at custom URL paths.
 */
@Singleton
public class RobotsController extends Controller {

  private final com.typesafe.config.Config configs;

  @Inject
  public RobotsController(com.typesafe.config.Config configs) {
    this.configs = configs;
  }

  /**
   * Serves robots.txt with dynamically generated paths based on the configured base path.
   *
   * @param request the HTTP request
   * @return robots.txt content with proper base path
   */
  public Result robots(Http.Request request) {
    String basePath = BasePathUtils.normalizeBasePath(configs.getString("datahub.basePath"));

    // Ensure base path ends with / for robots.txt format
    String basePathForRobots = basePath.isEmpty() ? "/" : basePath + "/";

    return ok(views.txt.robots.render(basePathForRobots))
        .as("text/plain")
        .withHeader("Cache-Control", "public, max-age=86400"); // Cache for 24 hours
  }
}
