package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationConfiguration;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityLineageResult;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.datahub.graphql.generated.LineageInput;
import com.linkedin.datahub.graphql.generated.LineageRelationship;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.SiblingGraphService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Lineage neighbors the actor may not view must come back as {@link Restricted} placeholders with
 * encrypted URNs when view authorization is enabled, and an unviewable source discloses no lineage.
 */
public class EntityLineageResultResolverTest {

  private static final Urn SOURCE =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,eng.orders,PROD)");
  private static final Urn VIEWABLE =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,eng.raw_orders,PROD)");
  private static final Urn HIDDEN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,finance.ledger,PROD)");
  private static final Urn ENCRYPTED = UrnUtils.getUrn("urn:li:restricted:opaque");

  private SiblingGraphService siblingGraphService;
  private RestrictedService restrictedService;
  private DataFetchingEnvironment env;

  @BeforeMethod
  public void setup() {
    siblingGraphService = mock(SiblingGraphService.class);
    restrictedService = mock(RestrictedService.class);
    when(restrictedService.encryptRestrictedUrn(HIDDEN)).thenReturn(ENCRYPTED);

    com.linkedin.metadata.graph.EntityLineageResult graphResult =
        new com.linkedin.metadata.graph.EntityLineageResult()
            .setStart(0)
            .setCount(2)
            .setTotal(2)
            .setFiltered(0)
            .setRelationships(
                new LineageRelationshipArray(relationship(VIEWABLE), relationship(HIDDEN)));
    when(siblingGraphService.getLineage(
            any(OperationContext.class),
            eq(SOURCE),
            any(),
            anyInt(),
            any(),
            anyInt(),
            anyBoolean(),
            anyBoolean(),
            anySet()))
        .thenReturn(graphResult);

    Dataset source = new Dataset();
    source.setUrn(SOURCE.toString());
    source.setType(EntityType.DATASET);
    LineageInput input = new LineageInput();
    input.setDirection(LineageDirection.UPSTREAM);

    env = mock(DataFetchingEnvironment.class);
    when(env.getSource()).thenReturn(source);
    when(env.getArgument("input")).thenReturn(input);
  }

  private static com.linkedin.metadata.graph.LineageRelationship relationship(Urn urn) {
    return new com.linkedin.metadata.graph.LineageRelationship()
        .setEntity(urn)
        .setType("DownstreamOf")
        .setDegree(1);
  }

  private static AuthorizationConfiguration viewAuthorization(boolean enabled) {
    AuthorizationConfiguration configuration = new AuthorizationConfiguration();
    configuration.setView(ViewAuthorizationConfiguration.builder().enabled(enabled).build());
    return configuration;
  }

  /** A real user session with view authorization on; VIEW is granted only on {@code viewable}. */
  private static QueryContext userContext(boolean viewEnabled, Set<Urn> viewable) {
    Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              boolean allowed =
                  request
                      .getResourceSpec()
                      .map(spec -> viewable.contains(UrnUtils.getUrn(spec.getEntity())))
                      .orElse(false);
              return new AuthorizationResult(
                  request,
                  allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                  "");
            });
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(aspectRetriever)
            .cachingAspectRetriever(
                TestOperationContexts.emptyActiveUsersAspectRetriever(
                    aspectRetriever::getEntityRegistry))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .build();
    OperationContext systemContext =
        TestOperationContexts.systemContext(
            () ->
                OperationContextConfig.builder()
                    .viewAuthorizationConfiguration(
                        ViewAuthorizationConfiguration.builder().enabled(viewEnabled).build())
                    .build(),
            null,
            null,
            null,
            () -> retrieverContext,
            null,
            null,
            null);
    OperationContext opContext =
        systemContext.asSession(
            RequestContext.TEST,
            authorizer,
            new Authentication(new Actor(ActorType.USER, "reader"), ""));
    QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext()).thenReturn(opContext);
    when(context.getActorUrn()).thenReturn("urn:li:corpuser:reader");
    return context;
  }

  private EntityLineageResultResolver resolver(boolean viewEnabled) {
    return new EntityLineageResultResolver(
        siblingGraphService, restrictedService, viewAuthorization(viewEnabled));
  }

  @Test
  public void testUnviewableNeighborIsRestrictedWithEncryptedUrn() throws Exception {
    QueryContext context = userContext(true, Set.of(SOURCE, VIEWABLE));
    when(env.getContext()).thenReturn(context);

    EntityLineageResult result = resolver(true).get(env).get();

    assertEquals(result.getTotal(), 2);
    List<LineageRelationship> relationships = result.getRelationships();
    assertEquals(relationships.size(), 2);
    assertTrue(relationships.get(0).getEntity() instanceof Dataset);
    assertEquals(relationships.get(0).getEntity().getUrn(), VIEWABLE.toString());
    assertTrue(relationships.get(1).getEntity() instanceof Restricted);
    assertEquals(relationships.get(1).getEntity().getUrn(), ENCRYPTED.toString());
    assertEquals(relationships.get(1).getEntity().getType(), EntityType.RESTRICTED);
  }

  @Test
  public void testAllNeighborsTypedWhenViewAuthorizationDisabled() throws Exception {
    // Deny-everything authorizer: nothing may be consulted when the feature is off.
    QueryContext context = userContext(false, Set.of());
    when(env.getContext()).thenReturn(context);

    EntityLineageResult result = resolver(false).get(env).get();

    assertEquals(result.getRelationships().size(), 2);
    assertTrue(result.getRelationships().stream().allMatch(r -> r.getEntity() instanceof Dataset));
    assertEquals(result.getRelationships().get(1).getEntity().getUrn(), HIDDEN.toString());
  }

  @Test
  public void testUnviewableSourceDisclosesNoLineage() throws Exception {
    QueryContext context = userContext(true, Set.of(VIEWABLE, HIDDEN));
    when(env.getContext()).thenReturn(context);

    EntityLineageResult result = resolver(true).get(env).get();

    assertTrue(result.getRelationships().isEmpty());
    assertEquals(result.getTotal(), 0);
    Mockito.verifyNoInteractions(siblingGraphService);
  }
}
