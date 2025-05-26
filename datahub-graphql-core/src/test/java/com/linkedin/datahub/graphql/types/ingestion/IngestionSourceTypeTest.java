package com.linkedin.datahub.graphql.types.ingestion;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataHubIngestionSourceKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IngestionSourceTypeTest {

  private static final String TEST_1_URN = "urn:li:dataHubIngestionSource:id-1";
  private static final String TEST_1_RECIPE = "{\"param\":\"value\"}";
  private static final String TEST_1_NAME = "test1";
  private static final String TEST_1_TYPE = "testType1";
  private static final DataHubIngestionSourceKey TEST_1_KEY =
      new DataHubIngestionSourceKey().setId("id-1");
  private static final DataHubIngestionSourceConfig TEST_1_CONFIG =
      new DataHubIngestionSourceConfig().setRecipe(TEST_1_RECIPE);
  private static final DataHubIngestionSourceInfo TEST_1_INFO =
      new DataHubIngestionSourceInfo()
          .setName(TEST_1_NAME)
          .setType(TEST_1_TYPE)
          .setConfig(TEST_1_CONFIG);

  private static final String TEST_2_URN = "urn:li:dataHubIngestionSource:id-2";

  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Urn ingestionSourceUrn1 = Urn.createFromString(TEST_1_URN);
    Urn ingestionSourceUrn2 = Urn.createFromString(TEST_2_URN);

    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq(
                    new HashSet<>(ImmutableSet.of(ingestionSourceUrn1, ingestionSourceUrn2))),
                Mockito.eq(IngestionSourceType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                ingestionSourceUrn1,
                new EntityResponse()
                    .setEntityName(Constants.INGESTION_SOURCE_ENTITY_NAME)
                    .setUrn(ingestionSourceUrn1)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.INGESTION_SOURCE_KEY_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(TEST_1_KEY.data())),
                                Constants.INGESTION_INFO_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(TEST_1_INFO.data())))))));

    IngestionSourceType type = new IngestionSourceType(client);

    QueryContext mockContext = getMockAllowContext();
    List<DataFetcherResult<IngestionSource>> result =
        type.batchLoad(ImmutableList.of(TEST_1_URN, TEST_2_URN), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(ingestionSourceUrn1, ingestionSourceUrn2)),
            Mockito.eq(IngestionSourceType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    IngestionSource ingestionSource1 = result.get(0).getData();
    assertEquals(ingestionSource1.getUrn(), TEST_1_URN);
    assertEquals(ingestionSource1.getType(), TEST_1_TYPE);
    assertEquals(ingestionSource1.getName(), TEST_1_NAME);
    assertEquals(ingestionSource1.getConfig().getRecipe(), TEST_1_RECIPE);

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
    IngestionSourceType type = new IngestionSourceType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () -> type.batchLoad(ImmutableList.of(TEST_1_URN, TEST_2_URN), context));
  }
}
