package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationConfiguration;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityLineageResult;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.LineageRelationship;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.SiblingGraphService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** With view authorization on, lineage shows neighbours the caller can't view as Restricted. */
public class EntityLineageResultResolverRestrictedTest {

  private static final Urn VISIBLE_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,visible,PROD)");
  private static final Urn HIDDEN_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,hidden,PROD)");
  private static final Urn BEYOND_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,beyond,PROD)");

  private SiblingGraphService siblingGraphService;
  private RestrictedService restrictedService;
  private EntityLineageResultResolver resolver;

  @BeforeMethod
  public void setup() {
    siblingGraphService = mock(SiblingGraphService.class);
    restrictedService = mock(RestrictedService.class);
    when(restrictedService.encryptRestrictedUrn(any(Urn.class)))
        .thenAnswer(
            invocation ->
                UrnUtils.getUrn(
                    "urn:li:restricted:"
                        + invocation.getArgument(0, Urn.class).getId().hashCode()));
    when(restrictedService.decryptRestrictedUrn(eq(encrypted(HIDDEN_URN)))).thenReturn(HIDDEN_URN);
    AuthorizationConfiguration authorizationConfiguration = new AuthorizationConfiguration();
    authorizationConfiguration.setView(
        ViewAuthorizationConfiguration.builder().enabled(true).build());
    resolver =
        new EntityLineageResultResolver(
            siblingGraphService, restrictedService, authorizationConfiguration, null);
  }

  @Test
  public void testUnviewableNeighbourIsRestricted() throws Exception {
    lineageOf(VISIBLE_URN, HIDDEN_URN);

    final EntityLineageResult result = resolver.get(environment(datasetSource(VISIBLE_URN))).get();

    final LineageRelationship relationship = result.getRelationships().get(0);
    assertTrue(relationship.getEntity() instanceof Restricted);
    assertEquals(relationship.getEntity().getUrn(), encrypted(HIDDEN_URN).toString());
    assertEquals(
        pathUrns(relationship), List.of(VISIBLE_URN.toString(), encrypted(HIDDEN_URN).toString()));
  }

  @Test
  public void testRestrictedNodeContinuesLineageWithoutRevealingItself() throws Exception {
    lineageOf(HIDDEN_URN, BEYOND_URN);
    final Restricted source = new Restricted();
    source.setUrn(encrypted(HIDDEN_URN).toString());
    source.setType(EntityType.RESTRICTED);

    final EntityLineageResult result = resolver.get(environment(source)).get();

    final LineageRelationship relationship = result.getRelationships().get(0);
    assertEquals(relationship.getEntity().getUrn(), BEYOND_URN.toString());
    assertEquals(
        pathUrns(relationship), List.of(encrypted(HIDDEN_URN).toString(), BEYOND_URN.toString()));
  }

  @Test
  public void testUnviewableSourceHasNoLineage() throws Exception {
    final EntityLineageResult result = resolver.get(environment(datasetSource(HIDDEN_URN))).get();

    assertEquals((int) result.getTotal(), 0);
    assertTrue(result.getRelationships().isEmpty());
    verify(siblingGraphService, never())
        .getLineage(
            any(), any(), any(), anyInt(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
  }

  private Urn encrypted(final Urn urn) {
    return UrnUtils.getUrn("urn:li:restricted:" + urn.getId().hashCode());
  }

  private static List<String> pathUrns(final LineageRelationship relationship) {
    return relationship.getPaths().get(0).getPath().stream()
        .map(Entity::getUrn)
        .collect(Collectors.toList());
  }

  private void lineageOf(final Urn from, final Urn to) {
    final com.linkedin.metadata.graph.LineageRelationship relationship =
        new com.linkedin.metadata.graph.LineageRelationship()
            .setEntity(to)
            .setType("DownstreamOf")
            .setDegree(1)
            .setPaths(new UrnArrayArray(new UrnArray(from, to)));
    when(siblingGraphService.getLineage(
            any(OperationContext.class),
            eq(from),
            eq(LineageDirection.UPSTREAM),
            anyInt(),
            anyInt(),
            anyInt(),
            anyBoolean(),
            anyBoolean(),
            any(Set.class)))
        .thenReturn(
            new com.linkedin.metadata.graph.EntityLineageResult()
                .setStart(0)
                .setCount(1)
                .setTotal(1)
                .setFiltered(0)
                .setRelationships(new LineageRelationshipArray(relationship)));
  }

  private static Dataset datasetSource(final Urn urn) {
    final Dataset source = new Dataset();
    source.setUrn(urn.toString());
    source.setType(EntityType.DATASET);
    return source;
  }

  private static DataFetchingEnvironment environment(final Entity source) {
    final QueryContext context = contextHiding(HIDDEN_URN);
    final DataFetchingEnvironment environment = mock(DataFetchingEnvironment.class);
    when(environment.getContext()).thenReturn(context);
    when(environment.getSource()).thenReturn(source);
    when(environment.getArgument(eq("input")))
        .thenReturn(ImmutableMap.of("direction", "UPSTREAM", "start", 0, "count", 100));
    return environment;
  }

  private static QueryContext contextHiding(final Urn hidden) {
    final Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              final AuthorizationRequest request = invocation.getArgument(0);
              final boolean denied =
                  request
                      .getResourceSpec()
                      .map(EntitySpec::getEntity)
                      .filter(hidden.toString()::equals)
                      .isPresent();
              return new AuthorizationResult(
                  request,
                  denied ? AuthorizationResult.Type.DENY : AuthorizationResult.Type.ALLOW,
                  "");
            });
    final Authentication authentication = new Authentication(new Actor(ActorType.USER, "test"), "");
    final OperationContext opContext =
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
                null)
            .asSession(RequestContext.TEST, authorizer, authentication);
    final QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext()).thenReturn(opContext);
    when(context.getActorUrn()).thenReturn("urn:li:corpuser:test");
    return context;
  }
}
