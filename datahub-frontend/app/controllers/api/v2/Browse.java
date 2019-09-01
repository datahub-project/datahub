package controllers.api.v2;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.dao.DaoFactory;
import com.linkedin.datahub.dao.view.BrowseDAO;
import controllers.Secured;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import play.Logger;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Security;
import utils.ControllerUtil;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.isBlank;


public class Browse extends Controller {

  private static final String REQUEST_TYPE = "type";
  private static final String REQUEST_PATH = "path";
  private static final String REQUEST_START = "start";
  private static final String REQUEST_COUNT = "count";
  private static final String REQUEST_URN = "urn";

  private static final String DATASET_ENTITY_TYPE = "dataset";

  private static final String DEFAULT_PATH = "";
  private static final int DEFAULT_START_VALUE = 0;
  private static final int DEFAULT_PAGE_SIZE = 10;

  private final Map<String, BrowseDAO> _browseDAOMap;

  private static final ImmutableMap<String, Set<String>> FILTER_FIELDS =
      ImmutableMap.of();

  private static Map<String, String> buildRequestMap(String type) {
    Map<String, String> requestMap = new HashMap<>();
    Set<String> facets = FILTER_FIELDS.getOrDefault(type, Collections.emptySet());
    facets.stream().forEach(facet -> requestMap.put(facet, request().getQueryString(facet)));
    return requestMap;
  }

  public Browse() {
    _browseDAOMap = ImmutableMap.<String, BrowseDAO>builder()
        .put(DATASET_ENTITY_TYPE, DaoFactory.getDatasetBrowseDAO())
        .build();
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result browse() {
    try {
      String type = request().getQueryString(REQUEST_TYPE);
      if (isBlank(type)) {
        return badRequest("Bad Request. type parameter can not be null");
      }

      String input = StringUtils.defaultIfBlank(request().getQueryString(REQUEST_PATH), DEFAULT_PATH);
      int start = NumberUtils.toInt(request().getQueryString(REQUEST_START), DEFAULT_START_VALUE);
      int count = NumberUtils.toInt(request().getQueryString(REQUEST_COUNT), DEFAULT_PAGE_SIZE);

      Map<String, String> requestMap = buildRequestMap(type);
      BrowseDAO browseDAO = _browseDAOMap.get(type);

      if (browseDAO != null) {
        return ok(browseDAO.browse(input, requestMap, start, count));
      } else {
        return badRequest("Bad Request. Type is not supported");
      }
    } catch (Exception e) {
      Logger.error("Browse failed: ", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getBrowsePaths() {
    try {
      String type = request().getQueryString(REQUEST_TYPE);
      String urn = request().getQueryString(REQUEST_URN);
      if (isBlank(type) || isBlank(urn)) {
        return badRequest("Bad Request. Type or urn parameter can not be null");
      }

      BrowseDAO browseDAO = _browseDAOMap.get(type);
      if (browseDAO != null) {
        return ok(browseDAO.getBrowsePaths(urn));
      } else {
        return badRequest("Bad Request. Type is not supported");
      }
    } catch (Exception e) {
      Logger.error("Browse paths request failed: ", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }
  }
}
