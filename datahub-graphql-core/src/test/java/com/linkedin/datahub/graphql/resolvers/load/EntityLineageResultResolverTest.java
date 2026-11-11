package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthorizationConfiguration;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityLineageResult;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.SiblingGraphService;
import graphql.execution.MergedField;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.OperationDefinition;
import graphql.parser.Parser;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** A lineage request that selects only totals is answered by the count path when enabled. */
public class EntityLineageResultResolverTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,resolver_count,PROD)");

  private SiblingGraphService siblingGraphService;
  private FeatureFlags featureFlags;
  private EntityLineageResultResolver resolver;
  private DataFetchingEnvironment environment;

  @BeforeMethod
  public void setup() {
    siblingGraphService = mock(SiblingGraphService.class);
    featureFlags = new FeatureFlags();
    featureFlags.setLineageCountOnlyEnabled(true);
    AuthorizationConfiguration authorizationConfiguration = new AuthorizationConfiguration();
    authorizationConfiguration.setView(
        ViewAuthorizationConfiguration.builder().enabled(false).build());
    resolver =
        new EntityLineageResultResolver(
            siblingGraphService,
            mock(RestrictedService.class),
            authorizationConfiguration,
            featureFlags);

    Dataset source = new Dataset();
    source.setUrn(DATASET_URN.toString());
    source.setType(EntityType.DATASET);
    QueryContext context = getMockAllowContext();
    environment = mock(DataFetchingEnvironment.class);
    when(environment.getContext()).thenReturn(context);
    when(environment.getSource()).thenReturn(source);
    when(environment.getArgument(eq("input")))
        .thenReturn(ImmutableMap.of("direction", "UPSTREAM", "start", 0, "count", 100));

    com.linkedin.metadata.graph.EntityLineageResult counted =
        new com.linkedin.metadata.graph.EntityLineageResult()
            .setStart(0)
            .setCount(100)
            .setTotal(13)
            .setFiltered(2)
            .setRelationships(new LineageRelationshipArray());
    when(siblingGraphService.getLineageCount(
            any(OperationContext.class),
            eq(DATASET_URN),
            eq(LineageDirection.UPSTREAM),
            anyInt(),
            anyBoolean(),
            anyBoolean()))
        .thenReturn(counted);
    when(siblingGraphService.getLineage(
            any(OperationContext.class),
            eq(DATASET_URN),
            eq(LineageDirection.UPSTREAM),
            anyInt(),
            anyInt(),
            anyInt(),
            anyBoolean(),
            anyBoolean(),
            any(Set.class)))
        .thenReturn(counted);
  }

  @Test
  public void testTotalsOnlySelectionUsesTheCountPath() throws Exception {
    selecting("{ lineage { total filtered } }");

    EntityLineageResult result = resolver.get(environment).get();

    assertEquals(result.getTotal().intValue(), 13);
    assertEquals(result.getFiltered().intValue(), 2);
    assertEquals(result.getRelationships().size(), 0);
    // The normalized selection set expands the whole operation; on a search page that exceeds
    // graphql-java's field limit, so the resolver must never ask for it.
    verify(environment, never()).getSelectionSet();
    verify(siblingGraphService, never())
        .getLineage(
            any(OperationContext.class),
            any(Urn.class),
            any(LineageDirection.class),
            anyInt(),
            anyInt(),
            anyInt(),
            anyBoolean(),
            anyBoolean(),
            any(Set.class));
  }

  @Test
  public void testRelationshipsSelectionWalksTheGraph() throws Exception {
    selecting("{ lineage { total relationships { type } } }");

    resolver.get(environment).get();

    verify(siblingGraphService, never())
        .getLineageCount(
            any(OperationContext.class),
            any(Urn.class),
            any(LineageDirection.class),
            anyInt(),
            anyBoolean(),
            anyBoolean());
  }

  @Test
  public void testFlagOffWalksTheGraph() throws Exception {
    featureFlags.setLineageCountOnlyEnabled(false);
    selecting("{ lineage { total filtered } }");

    resolver.get(environment).get();

    verify(siblingGraphService, never())
        .getLineageCount(
            any(OperationContext.class),
            any(Urn.class),
            any(LineageDirection.class),
            anyInt(),
            anyBoolean(),
            anyBoolean());
  }

  @Test
  public void testRelationshipsFoundThroughFragments() {
    selecting("{ lineage { total ... on EntityLineageResult { relationships { type } } } }");
    assertTrue(EntityLineageResultResolver.selectsRelationships(environment));

    selecting(
        "{ lineage { total ...rels } }",
        "fragment rels on EntityLineageResult { filtered ...deeper }",
        "fragment deeper on EntityLineageResult { relationships { type } }");
    assertTrue(EntityLineageResultResolver.selectsRelationships(environment));

    selecting("{ lineage { ...counts } }", "fragment counts on EntityLineageResult { total }");
    assertFalse(EntityLineageResultResolver.selectsRelationships(environment));

    // A fragment the request does not define cannot be inspected: take the full walk.
    selecting("{ lineage { ...missing } }");
    assertTrue(EntityLineageResultResolver.selectsRelationships(environment));
  }

  /** Points the environment at the {@code lineage} field of {@code query}. */
  private void selecting(String query, String... fragments) {
    Document document = Parser.parse(query + " " + String.join(" ", fragments));
    OperationDefinition operation = document.getDefinitionsOfType(OperationDefinition.class).get(0);
    Field lineage = (Field) operation.getSelectionSet().getSelections().get(0);
    Map<String, FragmentDefinition> byName =
        document.getDefinitionsOfType(FragmentDefinition.class).stream()
            .collect(Collectors.toMap(FragmentDefinition::getName, f -> f));
    when(environment.getMergedField()).thenReturn(MergedField.newMergedField(lineage).build());
    when(environment.getFragmentsByName()).thenReturn(byName);
  }

  @Test
  public void testExplicitNullIncludeGhostEntitiesDefaultsToFalse() throws Exception {
    SiblingGraphService siblingGraphService = mock(SiblingGraphService.class);
    when(siblingGraphService.getLineage(
            any(),
            eq(DATASET_URN),
            eq(LineageDirection.UPSTREAM),
            anyInt(),
            anyInt(),
            anyInt(),
            anyBoolean(),
            anyBoolean(),
            anySet()))
        .thenReturn(
            new com.linkedin.metadata.graph.EntityLineageResult()
                .setStart(0)
                .setCount(0)
                .setTotal(0)
                .setRelationships(new LineageRelationshipArray()));

    EntityLineageResultResolver resolver =
        new EntityLineageResultResolver(
            siblingGraphService,
            mock(RestrictedService.class),
            new AuthorizationConfiguration(),
            new FeatureFlags());

    // An explicit null bypasses the schema default (`includeGhostEntities: Boolean = false`).
    Map<String, Object> input = new HashMap<>();
    input.put("direction", "UPSTREAM");
    input.put("includeGhostEntities", null);

    Dataset source = new Dataset();
    source.setUrn(DATASET_URN.toString());

    QueryContext context = getMockAllowContext();
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(context);
    when(env.getSource()).thenReturn(source);
    when(env.getArgument("input")).thenReturn(input);

    EntityLineageResult result = resolver.get(env).get();

    assertEquals(result.getTotal(), 0);
    verify(siblingGraphService)
        .getLineage(
            any(),
            eq(DATASET_URN),
            eq(LineageDirection.UPSTREAM),
            eq(0),
            eq(100),
            eq(1),
            eq(false),
            eq(false),
            anySet());
  }
}
