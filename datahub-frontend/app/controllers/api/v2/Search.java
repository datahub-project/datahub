package controllers.api.v2;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.dao.DaoFactory;
import com.linkedin.datahub.dao.view.DocumentSearchDao;
import controllers.Secured;
import org.apache.commons.lang3.math.NumberUtils;
import play.Logger;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Security;
import utils.ControllerUtil;
import utils.SearchUtil;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class Search extends Controller {
  private static final String REQUEST_TYPE = "type";
  private static final String REQUEST_INPUT = "input";
  private static final String REQUEST_FIELD = "field";
  private static final String REQUEST_START = "start";
  private static final String REQUEST_COUNT = "count";
  private static final String REQUEST_LIMIT = "limit";

  private static final String CORP_USER_TYPE = "corpuser";
  private static final String DATASET_TYPE = "dataset";

  private static final Set<String> CORP_USER_FACET_FIELDS = Collections.emptySet();
  private static final Set<String> DATASET_FACET_FIELDS =
      ImmutableSet.of("origin", "platform");
  private static final ImmutableMap<String, Set<String>> FACET_FIELDS = ImmutableMap.of(
      CORP_USER_TYPE, CORP_USER_FACET_FIELDS,
      DATASET_TYPE, DATASET_FACET_FIELDS
  );

  private static final int _DEFAULT_START_VALUE = 0;
  private static final int _DEFAULT_PAGE_SIZE = 10;
  private static final int _DEFAULT_LIMIT_VALUE = 20;

  private static Map<String, String> buildRequestMap(String type) {
    Map<String, String> requestMap = new HashMap<>();
    Set<String> facets = FACET_FIELDS.getOrDefault(type, Collections.emptySet());
    facets.stream().forEach(facet -> requestMap.put(facet, request().getQueryString(facet)));
    return requestMap;
  }

  private final Map<String, DocumentSearchDao> _searchDaoMap;

  public Search() {
    _searchDaoMap = ImmutableMap.<String, DocumentSearchDao>builder()
        .put(CORP_USER_TYPE, DaoFactory.getCorpUserDocumentSearchDao())
        .put(DATASET_TYPE, DaoFactory.getDatasetDocumentSearchDao())
        .build();
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result search() {
    try {
      String type = request().getQueryString(REQUEST_TYPE);
      if (isBlank(type)) {
        return badRequest("Bad Request. type parameter can not be null or empty");
      }

      // escape forward slash since it is a reserved character in Elasticsearch
      // TODO: Once apis for exact or advanced(with support for field specific/regex) search are exposed,
      //  update to call appropriate api based on indication from user.
      final String input = SearchUtil
              .escapeForwardSlash(Strings.nullToEmpty(request().getQueryString(REQUEST_INPUT)));
      if (isBlank(input)) {
        return badRequest("Bad Request. input parameter can not be null or empty");
      }

      int start = NumberUtils.toInt(request().getQueryString(REQUEST_START), _DEFAULT_START_VALUE);
      int count = NumberUtils.toInt(request().getQueryString(REQUEST_COUNT), _DEFAULT_PAGE_SIZE);

      Map<String, String> requestMap = buildRequestMap(type);
      DocumentSearchDao documentSearchDao = _searchDaoMap.get(type);

      if (documentSearchDao != null) {
        return ok(documentSearchDao.search(input, requestMap, start, count));
      } else {
        return badRequest("Bad Request. Type is not supported");
      }
    } catch (Exception e) {
      Logger.error("Failed in documents search.", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result autoComplete() {
    try {

      String type = request().getQueryString(REQUEST_TYPE);
      if (isBlank(type)) {
        return badRequest("Bad Request. type parameter can not be null or empty");
      }

      String field = request().getQueryString(REQUEST_FIELD);
      if (isBlank(field)) {
        return badRequest("Bad Request. field parameter can not be null or empty");
      }

      // escape forward slash since it is a reserved character in Elasticsearch
      // TODO: Once apis for exact or advanced(with support for field specific/regex) search are exposed,
      //  update to call appropriate api based on indication from user.
      final String input = SearchUtil
              .escapeForwardSlash(Strings.nullToEmpty(request().getQueryString(REQUEST_INPUT)));

      int limit = NumberUtils.toInt(request().getQueryString(REQUEST_LIMIT), _DEFAULT_LIMIT_VALUE);

      Map<String, String> requestMap = buildRequestMap(type);
      DocumentSearchDao documentSearchDao = _searchDaoMap.get(type);

      if (documentSearchDao != null) {
        return ok(documentSearchDao.autoComplete(input, field, requestMap, limit));
      } else {
        return badRequest("Bad Request. Type is not supported");
      }
    } catch (Exception e) {
      Logger.error("Failed in auto complete request.", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }
  }
}
