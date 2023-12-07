package io.datahubproject.openapi.delegates;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.generated.controller.DatahubUsageEventsApiDelegate;
import java.util.Objects;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;

public class DatahubUsageEventsImpl implements DatahubUsageEventsApiDelegate {

  @Autowired private ElasticSearchService _searchService;
  @Autowired private AuthorizerChain _authorizationChain;

  @Value("${authorization.restApiAuthorization:false}")
  private boolean _restApiAuthorizationEnabled;

  public static final String DATAHUB_USAGE_INDEX = "datahub_usage_event";

  @Override
  public ResponseEntity<String> raw(String body) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    checkAnalyticsAuthorized(authentication);
    return ResponseEntity.of(_searchService.raw(DATAHUB_USAGE_INDEX, body).map(Objects::toString));
  }

  private void checkAnalyticsAuthorized(Authentication authentication) {
    String actorUrnStr = authentication.getActor().toUrnStr();
    DisjunctivePrivilegeGroup orGroup =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.GET_ANALYTICS_PRIVILEGE.getType()))));

    if (_restApiAuthorizationEnabled
        && !AuthUtil.isAuthorized(_authorizationChain, actorUrnStr, Optional.empty(), orGroup)) {
      throw new UnauthorizedException(actorUrnStr + " is unauthorized to get analytics.");
    }
  }
}
