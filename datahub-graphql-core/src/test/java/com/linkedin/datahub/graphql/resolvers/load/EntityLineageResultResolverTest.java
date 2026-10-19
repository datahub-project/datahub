package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

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
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Set;
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
  private DataFetchingFieldSelectionSet selectionSet;

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
    selectionSet = mock(DataFetchingFieldSelectionSet.class);
    environment = mock(DataFetchingEnvironment.class);
    when(environment.getContext()).thenReturn(context);
    when(environment.getSource()).thenReturn(source);
    when(environment.getSelectionSet()).thenReturn(selectionSet);
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
    when(selectionSet.contains("relationships")).thenReturn(false);

    EntityLineageResult result = resolver.get(environment).get();

    assertEquals(result.getTotal().intValue(), 13);
    assertEquals(result.getFiltered().intValue(), 2);
    assertEquals(result.getRelationships().size(), 0);
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
    when(selectionSet.contains("relationships")).thenReturn(true);

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
    when(selectionSet.contains("relationships")).thenReturn(false);

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
}
