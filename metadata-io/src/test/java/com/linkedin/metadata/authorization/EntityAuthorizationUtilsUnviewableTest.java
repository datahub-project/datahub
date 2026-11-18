package com.linkedin.metadata.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextAuthorizer;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.graphql.GraphQLOperationKind;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** The single evaluator behind every check that answers for unviewable entities as missing. */
public class EntityAuthorizationUtilsUnviewableTest {

  private static final Urn HIDDEN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,hidden,PROD)");

  private Authorizer authorizer;
  private OperationContextAuthorizer contextAuthorizer;

  @BeforeMethod
  public void setup() {
    authorizer =
        mock(Authorizer.class, withSettings().extraInterfaces(OperationContextAuthorizer.class));
    contextAuthorizer = (OperationContextAuthorizer) authorizer;
    when(contextAuthorizer.authorize(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              boolean denied =
                  request
                      .getResourceSpec()
                      .map(EntitySpec::getEntity)
                      .filter(HIDDEN.toString()::equals)
                      .isPresent();
              return new AuthorizationResult(
                  request,
                  denied ? AuthorizationResult.Type.DENY : AuthorizationResult.Type.ALLOW,
                  "");
            });
  }

  @Test
  public void testChecksManyUrnsWithOnePrefetchAndSeedsTheSessionCache() {
    OperationContext query = session(true);
    List<Urn> visible =
        IntStream.range(0, 10)
            .mapToObj(
                i -> UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,t" + i + ",PROD)"))
            .collect(Collectors.toList());
    List<Urn> urns = new ArrayList<>(visible);
    urns.add(HIDDEN);

    assertEquals(EntityAuthorizationUtils.unviewableUrns(query, urns), Set.of(HIDDEN));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Collection<EntitySpec>> prefetched = ArgumentCaptor.forClass(Collection.class);
    verify(contextAuthorizer, times(1)).prefetchResourceSpecs(prefetched.capture(), any(), any());
    assertEquals(
        prefetched.getValue().stream().map(EntitySpec::getEntity).collect(Collectors.toSet()),
        urns.stream().map(Urn::toString).collect(Collectors.toSet()));

    clearInvocations(contextAuthorizer);
    assertFalse(EntityAuthorizationUtils.canView(query, HIDDEN));
    assertTrue(EntityAuthorizationUtils.canView(query, visible.get(0)));
    verify(contextAuthorizer, never()).authorize(any(), any(), any());
  }

  @Test
  public void testSkipsRestrictedUrnsUnknownTypesAndInactiveViewAuthorization() {
    Urn restricted = UrnUtils.getUrn("urn:li:restricted:abc");
    Urn unknownType = UrnUtils.getUrn("urn:li:notAnEntityType:abc");

    assertTrue(
        EntityAuthorizationUtils.unviewableUrns(session(true), List.of(restricted, unknownType))
            .isEmpty());
    assertTrue(EntityAuthorizationUtils.unviewableUrns(session(false), List.of(HIDDEN)).isEmpty());
    verify(contextAuthorizer, never()).authorize(any(), any(), any());
    verify(contextAuthorizer, never()).prefetchResourceSpecs(anyCollection(), any(), any());
  }

  private OperationContext session(boolean viewAuthorizationEnabled) {
    return TestOperationContexts.systemContext(
            () ->
                OperationContextConfig.builder()
                    .viewAuthorizationConfiguration(
                        ViewAuthorizationConfiguration.builder()
                            .enabled(viewAuthorizationEnabled)
                            .build())
                    .build(),
            null,
            null,
            null,
            null,
            null,
            null,
            null)
        .asSession(
            RequestContext.TEST.build().toBuilder()
                .graphqlOperationKind(GraphQLOperationKind.QUERY),
            authorizer,
            new Authentication(new Actor(ActorType.USER, "test"), ""));
  }
}
