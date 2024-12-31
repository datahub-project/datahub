package com.linkedin.datahub.graphql.types.datacontract;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datacontract.DataContractProperties;
import com.linkedin.datacontract.DataContractState;
import com.linkedin.datacontract.DataContractStatus;
import com.linkedin.datacontract.DataQualityContract;
import com.linkedin.datacontract.DataQualityContractArray;
import com.linkedin.datacontract.FreshnessContract;
import com.linkedin.datacontract.FreshnessContractArray;
import com.linkedin.datacontract.SchemaContract;
import com.linkedin.datacontract.SchemaContractArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataContract;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataContractKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashSet;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DataContractTypeTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private static final Urn DATA_QUALITY_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:quality");
  private static final Urn FRESHNESS_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:freshness");
  private static final Urn SCHEMA_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:schema");
  private static final String TEST_DATA_CONTRACT_1_URN = "urn:li:dataContract:id-1";
  private static final DataContractKey TEST_DATA_CONTRACT_1_KEY =
      new DataContractKey().setId("id-1");
  private static final DataContractProperties TEST_DATA_CONTRACT_1_PROPERTIES =
      new DataContractProperties()
          .setEntity(TEST_DATASET_URN)
          .setDataQuality(
              new DataQualityContractArray(
                  ImmutableList.of(
                      new DataQualityContract().setAssertion(DATA_QUALITY_ASSERTION_URN))))
          .setFreshness(
              new FreshnessContractArray(
                  ImmutableList.of(new FreshnessContract().setAssertion(FRESHNESS_ASSERTION_URN))))
          .setSchema(
              new SchemaContractArray(
                  ImmutableList.of(new SchemaContract().setAssertion(SCHEMA_ASSERTION_URN))));
  private static final DataContractStatus TEST_DATA_CONTRACT_1_STATUS =
      new DataContractStatus().setState(DataContractState.ACTIVE);

  private static final String TEST_DATA_CONTRACT_2_URN = "urn:li:dataContract:id-2";

  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Urn dataContractUrn1 = Urn.createFromString(TEST_DATA_CONTRACT_1_URN);
    Urn dataContractUrn2 = Urn.createFromString(TEST_DATA_CONTRACT_2_URN);

    Mockito.when(
            client.batchGetV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATA_CONTRACT_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(dataContractUrn1, dataContractUrn2))),
                Mockito.eq(DataContractType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                dataContractUrn1,
                new EntityResponse()
                    .setEntityName(Constants.DATA_CONTRACT_ENTITY_NAME)
                    .setUrn(dataContractUrn1)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DATA_CONTRACT_KEY_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DATA_CONTRACT_1_KEY.data())),
                                Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DATA_CONTRACT_1_PROPERTIES.data())),
                                Constants.DATA_CONTRACT_STATUS_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DATA_CONTRACT_1_STATUS.data())))))));

    DataContractType type = new DataContractType(client);

    QueryContext mockContext = getMockAllowContext();
    List<DataFetcherResult<DataContract>> result =
        type.batchLoad(
            ImmutableList.of(TEST_DATA_CONTRACT_1_URN, TEST_DATA_CONTRACT_2_URN), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(OperationContext.class),
            Mockito.eq(Constants.DATA_CONTRACT_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(dataContractUrn1, dataContractUrn2)),
            Mockito.eq(DataContractType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    DataContract dataContract1 = result.get(0).getData();
    assertEquals(dataContract1.getUrn(), TEST_DATA_CONTRACT_1_URN);
    assertEquals(dataContract1.getType(), EntityType.DATA_CONTRACT);
    assertEquals(dataContract1.getProperties().getEntityUrn(), TEST_DATASET_URN.toString());
    assertEquals(dataContract1.getProperties().getDataQuality().size(), 1);
    assertEquals(dataContract1.getProperties().getSchema().size(), 1);
    assertEquals(dataContract1.getProperties().getFreshness().size(), 1);

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(
            nullable(OperationContext.class),
            Mockito.anyString(),
            Mockito.anySet(),
            Mockito.anySet());
    DataContractType type = new DataContractType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () ->
            type.batchLoad(
                ImmutableList.of(TEST_DATA_CONTRACT_1_URN, TEST_DATA_CONTRACT_2_URN), context));
  }
}
