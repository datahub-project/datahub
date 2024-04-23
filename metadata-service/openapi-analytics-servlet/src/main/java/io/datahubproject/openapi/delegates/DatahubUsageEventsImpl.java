package io.datahubproject.openapi.delegates;

import static com.linkedin.metadata.authorization.ApiGroup.ANALYTICS;
import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.v2.generated.controller.DatahubUsageEventsApiDelegate;
import java.util.List;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;

public class DatahubUsageEventsImpl implements DatahubUsageEventsApiDelegate {

  @Autowired private ElasticSearchService _searchService;
  @Autowired private AuthorizerChain _authorizationChain;

  @Autowired
  @Qualifier("systemOperationContext")
  OperationContext systemOperationContext;

  public static final String DATAHUB_USAGE_INDEX = "datahub_usage_event";

  @Override
  public ResponseEntity<String> raw(String body) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    checkAnalyticsAuthorized(authentication);
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("raw", List.of()),
            _authorizationChain,
            authentication,
            true);

    return ResponseEntity.of(
        _searchService.raw(opContext, DATAHUB_USAGE_INDEX, body).map(Objects::toString));
  }

  private void checkAnalyticsAuthorized(Authentication authentication) {
    if (!AuthUtil.isAPIAuthorized(authentication, _authorizationChain, ANALYTICS, READ)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to get analytics.");
    }
  }
}
