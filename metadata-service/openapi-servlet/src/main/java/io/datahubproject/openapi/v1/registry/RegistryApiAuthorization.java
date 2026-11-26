package io.datahubproject.openapi.v1.registry;

import static com.linkedin.metadata.Constants.ANONYMOUS_ACTOR_ID;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RegistryApiAuthorization {
  public static final String READ_ACCESS_DESCRIPTION =
      " Authenticated actors may read schema metadata by default. When "
          + "authorization.registryApi.authenticatedReadEnabled "
          + "(REGISTRY_API_AUTHENTICATED_READ_ENABLED) is false, requires MANAGE_SYSTEM_OPERATIONS.";
  public static final String FORBIDDEN_DESCRIPTION =
      "Caller lacks MANAGE_SYSTEM_OPERATIONS while REGISTRY_API_AUTHENTICATED_READ_ENABLED is false";

  private final AuthorizerChain authorizerChain;
  private final OperationContext systemOperationContext;
  private final ConfigurationProvider configurationProvider;

  public Optional<HttpStatus> getReadDenialStatus(HttpServletRequest request, String action) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    // The extraction filter supplies an anonymous actor when authentication fails.
    if (authentication == null
        || authentication.getActor() == null
        || ANONYMOUS_ACTOR_ID.equals(authentication.getActor().getId())) {
      return Optional.of(HttpStatus.UNAUTHORIZED);
    }

    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, action, Collections.emptyList())
                .withUsageOperation(UsageOperation.OTHER_READ),
            authorizerChain,
            authentication);

    if (!configurationProvider.getAuthorization().getRegistryApi().isAuthenticatedReadEnabled()
        && !AuthUtil.isAPIOperationsAuthorized(
            opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      return Optional.of(HttpStatus.FORBIDDEN);
    }
    return Optional.empty();
  }
}
