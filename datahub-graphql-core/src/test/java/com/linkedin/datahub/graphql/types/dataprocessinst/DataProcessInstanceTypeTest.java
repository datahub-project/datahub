package com.linkedin.datahub.graphql.types.dataprocessinst;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.FabricType;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.dataprocess.DataProcessInstanceInput;
import com.linkedin.dataprocess.DataProcessInstanceOutput;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.dataprocess.DataProcessInstanceRunEvent;
import com.linkedin.dataprocess.DataProcessRunStatus;
import com.linkedin.dataprocess.DataProcessType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataProcessInstanceKey;
import com.linkedin.ml.metadata.MLTrainingRunProperties;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.TestResult;
import com.linkedin.test.TestResultArray;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import graphql.execution.DataFetcherResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DataProcessInstanceTypeTest {

  private static final String TEST_INSTANCE_URN =
      "urn:li:dataProcessInstance:(test-workflow,test-instance-1)";
  private static final String TEST_DPI_1_URN = "urn:li:dataProcessInstance:id-1";
  private static final DatasetUrn DATASET_URN =
      new DatasetUrn(new DataPlatformUrn("kafka"), "dataset1", FabricType.TEST);
  private static final Urn DPI_URN_REL = UrnUtils.getUrn("urn:li:dataProcessInstance:id-2");
  private static final DataProcessInstanceKey TEST_DPI_1_KEY =
      new DataProcessInstanceKey().setId("id-1");
  private static final DataProcessInstanceProperties TEST_DPI_1_PROPERTIES =
      new DataProcessInstanceProperties().setName("Test DPI").setType(DataProcessType.STREAMING);
  private static final DataProcessInstanceInput TEST_DPI_1_DPI_INPUT =
      new DataProcessInstanceInput().setInputs(new UrnArray(ImmutableList.of(DATASET_URN)));
  private static final DataProcessInstanceOutput TEST_DPI_1_DPI_OUTPUT =
      new DataProcessInstanceOutput().setOutputs(new UrnArray(ImmutableList.of(DATASET_URN)));
  private static final DataProcessInstanceRelationships TEST_DPI_1_DPI_RELATIONSHIPS =
      new DataProcessInstanceRelationships()
          .setParentInstance(DPI_URN_REL)
          .setUpstreamInstances(new UrnArray(ImmutableList.of(DPI_URN_REL)))
          .setParentTemplate(DPI_URN_REL);
  private static final DataProcessInstanceRunEvent TEST_DPI_1_DPI_RUN_EVENT =
      new DataProcessInstanceRunEvent().setStatus(DataProcessRunStatus.COMPLETE);
  private static final DataPlatformInstance TEST_DPI_1_DATA_PLATFORM_INSTANCE =
      new DataPlatformInstance().setPlatform(new DataPlatformUrn("kafka"));
  private static final Status TEST_DPI_1_STATUS = new Status().setRemoved(false);
  private static final TestResults TEST_DPI_1_TEST_RESULTS =
      new TestResults()
          .setPassing(
              new TestResultArray(
                  ImmutableList.of(
                      new TestResult()
                          .setTest(UrnUtils.getUrn("urn:li:test:123"))
                          .setType(TestResultType.SUCCESS))))
          .setFailing(new TestResultArray());
  private static final SubTypes TEST_DPI_1_SUB_TYPES =
      new SubTypes().setTypeNames(new StringArray("subtype1"));
  private static final Container TEST_DPI_1_CONTAINER =
      new Container().setContainer(UrnUtils.getUrn("urn:li:container:123"));
  private static final MLTrainingRunProperties ML_TRAINING_RUN_PROPERTIES =
      new MLTrainingRunProperties().setId("mytrainingrun");

  private static final String TEST_DPI_2_URN = "urn:li:dataProcessInstance:id-2";

  @Test
  public void testBatchLoadFull() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    Urn dpiUrn1 = Urn.createFromString(TEST_DPI_1_URN);
    Urn dpiUrn2 = Urn.createFromString(TEST_DPI_2_URN);

    Map<String, EnvelopedAspect> aspectMap = new HashMap<>();
    aspectMap.put(
        Constants.DATA_PROCESS_INSTANCE_KEY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DPI_1_KEY.data())));
    aspectMap.put(
        Constants.DATA_PROCESS_INSTANCE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DPI_1_PROPERTIES.data())));
    aspectMap.put(
        Constants.DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DPI_1_DPI_INPUT.data())));
    aspectMap.put(
        Constants.DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DPI_1_DPI_OUTPUT.data())));
    aspectMap.put(
        Constants.DATA_PROCESS_INSTANCE_RELATIONSHIPS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DPI_1_DPI_RELATIONSHIPS.data())));
    aspectMap.put(
        Constants.DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DPI_1_DPI_RUN_EVENT.data())));
    aspectMap.put(
        Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DPI_1_DATA_PLATFORM_INSTANCE.data())));
    aspectMap.put(
        Constants.STATUS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DPI_1_STATUS.data())));
    aspectMap.put(
        Constants.TEST_RESULTS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DPI_1_TEST_RESULTS.data())));
    aspectMap.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DPI_1_SUB_TYPES.data())));
    aspectMap.put(
        Constants.CONTAINER_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DPI_1_CONTAINER.data())));
    aspectMap.put(
        Constants.ML_TRAINING_RUN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(ML_TRAINING_RUN_PROPERTIES.data())));

    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(dpiUrn1, dpiUrn2))),
                Mockito.eq(DataProcessInstanceType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                dpiUrn1,
                new EntityResponse()
                    .setEntityName(Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME)
                    .setUrn(dpiUrn1)
                    .setAspects(new EnvelopedAspectMap(aspectMap))));

    FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);
    Mockito.when(mockFeatureFlags.isDataProcessInstanceEntityEnabled()).thenReturn(true);

    DataProcessInstanceType type = new DataProcessInstanceType(client, mockFeatureFlags);

    QueryContext mockContext = getMockAllowContext();
    List<DataFetcherResult<DataProcessInstance>> result =
        type.batchLoad(ImmutableList.of(TEST_DPI_1_URN, TEST_DPI_2_URN), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(dpiUrn1, dpiUrn2)),
            Mockito.eq(DataProcessInstanceType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    DataProcessInstance dpi1 = result.get(0).getData();
    assertEquals(dpi1.getUrn(), TEST_DPI_1_URN);
    assertEquals(dpi1.getName(), "Test DPI");
    assertEquals(dpi1.getType(), EntityType.DATA_PROCESS_INSTANCE);

    // Assert second element is null
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoad() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);
    Mockito.when(mockFeatureFlags.isDataProcessInstanceEntityEnabled()).thenReturn(true);

    DataProcessInstanceType type = new DataProcessInstanceType(mockClient, mockFeatureFlags);

    List<DataFetcherResult<DataProcessInstance>> result =
        type.batchLoad(ImmutableList.of(TEST_INSTANCE_URN), getMockAllowContext());

    assertEquals(result.size(), 1);
  }

  @Test
  public void testBatchLoadFeatureFlagDisabled() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);
    Mockito.when(mockFeatureFlags.isDataProcessInstanceEntityEnabled()).thenReturn(false);

    DataProcessInstanceType type = new DataProcessInstanceType(mockClient, mockFeatureFlags);

    List<DataFetcherResult<DataProcessInstance>> result =
        type.batchLoad(ImmutableList.of(TEST_INSTANCE_URN), getMockAllowContext());

    assertEquals(result.size(), 0);

    Mockito.verify(mockClient, Mockito.never())
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);
    Mockito.when(mockFeatureFlags.isDataProcessInstanceEntityEnabled()).thenReturn(true);

    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());

    DataProcessInstanceType type = new DataProcessInstanceType(mockClient, mockFeatureFlags);
    type.batchLoad(ImmutableList.of(TEST_INSTANCE_URN), getMockAllowContext());
  }

  @Test
  public void testGetType() {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);
    DataProcessInstanceType type = new DataProcessInstanceType(mockClient, mockFeatureFlags);

    assertEquals(type.type(), EntityType.DATA_PROCESS_INSTANCE);
  }

  @Test
  public void testObjectClass() {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);
    DataProcessInstanceType type = new DataProcessInstanceType(mockClient, mockFeatureFlags);

    assertEquals(type.objectClass(), DataProcessInstance.class);
  }
}
