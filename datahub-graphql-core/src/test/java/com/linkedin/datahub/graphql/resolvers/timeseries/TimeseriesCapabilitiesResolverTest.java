package com.linkedin.datahub.graphql.resolvers.timeseries;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.Operation;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateFormInput;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.FormType;
import com.linkedin.datahub.graphql.generated.TimeseriesCapabilitiesResult;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetUsageStatistics;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.mxe.GenericAspect;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class TimeseriesCapabilitiesResolverTest {
  private static final CreateFormInput TEST_INPUT =
      new CreateFormInput(null, "test name", null, FormType.VERIFICATION, new ArrayList<>(), null);

  private static final Dataset TEST_SOURCE = new Dataset();
  private static final String TEST_DATASET_URN = "urn:li:dataset:(test,test,test)";

  static {
    TEST_SOURCE.setUrn(TEST_DATASET_URN);
  }

  @Test
  public void testGetSuccessAllResults() throws Exception {
    EntityClient mockEntityClient = initMockEntityClientAlData(true);
    TimeseriesCapabilitiesResolver resolver = new TimeseriesCapabilitiesResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    TimeseriesCapabilitiesResult result = resolver.get(mockEnv).get();

    assertEquals(result.getAssetStats().getOldestOperationTime(), 123L);
    assertEquals(result.getAssetStats().getOldestDatasetUsageTime(), 456L);
    assertEquals(result.getAssetStats().getOldestDatasetProfileTime(), 789L);
  }

  @Test
  public void testGetSuccessNoPermissions() throws Exception {
    EntityClient mockEntityClient = initMockEntityClientAlData(true);
    TimeseriesCapabilitiesResolver resolver = new TimeseriesCapabilitiesResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    TimeseriesCapabilitiesResult result = resolver.get(mockEnv).get();

    assertNull(result.getAssetStats().getOldestOperationTime());
    assertNull(result.getAssetStats().getOldestDatasetUsageTime());
    assertNull(result.getAssetStats().getOldestDatasetProfileTime());
  }

  @Test
  public void testGetSuccessNoResults() throws Exception {
    EntityClient mockEntityClient = initMockEntityClientAlData(false);
    TimeseriesCapabilitiesResolver resolver = new TimeseriesCapabilitiesResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    TimeseriesCapabilitiesResult result = resolver.get(mockEnv).get();

    assertNull(result.getAssetStats().getOldestOperationTime());
    assertNull(result.getAssetStats().getOldestDatasetUsageTime());
    assertNull(result.getAssetStats().getOldestDatasetProfileTime());
  }

  @Test
  public void testGetException() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockEntityClient.getTimeseriesAspectValues(
                any(),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                any(),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1),
                Mockito.eq(null),
                any()))
        .thenThrow(new RuntimeException("ERROR!"));

    TimeseriesCapabilitiesResolver resolver = new TimeseriesCapabilitiesResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    TimeseriesCapabilitiesResult result = resolver.get(mockEnv).get();

    // when the service throws, return null results to not block returning the rest of the asset
    assertNull(result.getAssetStats().getOldestOperationTime());
    assertNull(result.getAssetStats().getOldestDatasetUsageTime());
    assertNull(result.getAssetStats().getOldestDatasetProfileTime());
  }

  private EntityClient initMockEntityClientAlData(boolean hasResults) throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    JacksonDataTemplateCodec secondDataTemplate = new JacksonDataTemplateCodec();

    // return operation
    Operation operation = new Operation();
    operation.setTimestampMillis(123L);
    byte[] operationSerialized = secondDataTemplate.dataTemplateToBytes(operation);
    Mockito.when(
            client.getTimeseriesAspectValues(
                any(),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(Constants.OPERATION_ASPECT_NAME),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1),
                Mockito.eq(null),
                any()))
        .thenReturn(
            hasResults
                ? ImmutableList.of(
                    new EnvelopedAspect()
                        .setAspect(
                            new GenericAspect()
                                .setContentType("application/json")
                                .setValue(ByteString.unsafeWrap(operationSerialized))))
                : ImmutableList.of());

    // return usage
    DatasetUsageStatistics usage = new DatasetUsageStatistics();
    usage.setTimestampMillis(456L);
    byte[] usageSerialized = secondDataTemplate.dataTemplateToBytes(usage);
    Mockito.when(
            client.getTimeseriesAspectValues(
                any(),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(Constants.DATASET_USAGE_STATISTICS_ASPECT_NAME),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1),
                Mockito.eq(null),
                any()))
        .thenReturn(
            hasResults
                ? ImmutableList.of(
                    new EnvelopedAspect()
                        .setAspect(
                            new GenericAspect()
                                .setContentType("application/json")
                                .setValue(ByteString.unsafeWrap(usageSerialized))))
                : ImmutableList.of());

    // return profile
    DatasetProfile profile = new DatasetProfile();
    profile.setTimestampMillis(789L);
    byte[] profileSerialized = secondDataTemplate.dataTemplateToBytes(profile);
    Mockito.when(
            client.getTimeseriesAspectValues(
                any(),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(Constants.DATASET_PROFILE_ASPECT_NAME),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1),
                Mockito.eq(null),
                any()))
        .thenReturn(
            hasResults
                ? ImmutableList.of(
                    new EnvelopedAspect()
                        .setAspect(
                            new GenericAspect()
                                .setContentType("application/json")
                                .setValue(ByteString.unsafeWrap(profileSerialized))))
                : ImmutableList.of());

    return client;
  }
}
