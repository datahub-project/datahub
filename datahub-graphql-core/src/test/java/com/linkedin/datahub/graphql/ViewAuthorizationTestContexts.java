package com.linkedin.datahub.graphql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Set;
import java.util.stream.Collectors;

/** Query contexts for a user session with view authorization enabled. */
public final class ViewAuthorizationTestContexts {

  private ViewAuthorizationTestContexts() {}

  /** A user who may do anything except view the given entities. */
  public static QueryContext contextHiding(final Urn... hiddenUrns) {
    final Set<String> hidden =
        Set.of(hiddenUrns).stream().map(Urn::toString).collect(Collectors.toSet());
    final Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              final AuthorizationRequest request = invocation.getArgument(0);
              final boolean denied =
                  request
                      .getResourceSpec()
                      .map(EntitySpec::getEntity)
                      .filter(hidden::contains)
                      .isPresent();
              return new AuthorizationResult(
                  request,
                  denied ? AuthorizationResult.Type.DENY : AuthorizationResult.Type.ALLOW,
                  "");
            });

    final OperationContext systemContext =
        TestOperationContexts.systemContext(
            () ->
                OperationContextConfig.builder()
                    .viewAuthorizationConfiguration(
                        ViewAuthorizationConfiguration.builder().enabled(true).build())
                    .build(),
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    final Authentication authentication = new Authentication(new Actor(ActorType.USER, "test"), "");
    final OperationContext opContext =
        systemContext.asSession(RequestContext.TEST, authorizer, authentication);

    final QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext()).thenReturn(opContext);
    when(context.getAuthentication()).thenReturn(authentication);
    when(context.getActorUrn()).thenReturn("urn:li:corpuser:test");
    when(context.getAuthorizer()).thenReturn(authorizer);
    return context;
  }
}
