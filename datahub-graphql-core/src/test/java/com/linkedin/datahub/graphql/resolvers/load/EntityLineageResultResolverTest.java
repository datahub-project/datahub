package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityLineageResult;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.SiblingGraphService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class EntityLineageResultResolverTest {
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");

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
            siblingGraphService, mock(RestrictedService.class), new AuthorizationConfiguration());

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
