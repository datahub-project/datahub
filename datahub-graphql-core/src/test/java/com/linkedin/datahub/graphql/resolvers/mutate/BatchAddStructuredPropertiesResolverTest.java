package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchAddStructuredPropertiesInput;
import com.linkedin.datahub.graphql.generated.PropertyValueInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.generated.StructuredPropertyInputParams;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.testng.annotations.Test;

public class BatchAddStructuredPropertiesResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_PROPERTY_URN_1 = "urn:li:structuredProperty:test-property-1";
  private static final String TEST_PROPERTY_URN_2 = "urn:li:structuredProperty:test-property-2";

  @Test
  public void testGetSuccessNoExistingProperties() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any())).thenReturn(true);

    EntityResponse entityResponse1 = new EntityResponse();
    entityResponse1.setUrn(UrnUtils.getUrn(TEST_ENTITY_URN_1));
    entityResponse1.setEntityName("dataset");
    entityResponse1.setAspects(new EnvelopedAspectMap());

    EntityResponse entityResponse2 = new EntityResponse();
    entityResponse2.setUrn(UrnUtils.getUrn(TEST_ENTITY_URN_2));
    entityResponse2.setEntityName("dataset");
    entityResponse2.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(any(), any(), any(), any()))
        .thenReturn(entityResponse1)
        .thenReturn(entityResponse2);

    BatchAddStructuredPropertiesResolver resolver =
        new BatchAddStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchAddStructuredPropertiesInput input =
        new BatchAddStructuredPropertiesInput(
            ImmutableList.of(
                new StructuredPropertyInputParams(
                    TEST_PROPERTY_URN_1,
                    ImmutableList.of(new PropertyValueInput("test-value", null)))),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    verify(mockClient, times(2)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testGetSuccessWithExistingProperties() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any())).thenReturn(true);

    // Create entity response with existing structured properties
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(TEST_ENTITY_URN_1));
    entityResponse.setEntityName("dataset");

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect structuredPropsAspect = new EnvelopedAspect();

    // Create existing structured properties
    StructuredProperties existingProps = new StructuredProperties();
    StructuredPropertyValueAssignment existingAssignment = new StructuredPropertyValueAssignment();
    existingAssignment.setPropertyUrn(UrnUtils.getUrn(TEST_PROPERTY_URN_2));
    existingProps.setProperties(
        new StructuredPropertyValueAssignmentArray(ImmutableList.of(existingAssignment)));

    structuredPropsAspect.setValue(new Aspect(existingProps.data()));
    aspectMap.put("structuredProperties", structuredPropsAspect);
    entityResponse.setAspects(aspectMap);

    when(mockClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    BatchAddStructuredPropertiesResolver resolver =
        new BatchAddStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchAddStructuredPropertiesInput input =
        new BatchAddStructuredPropertiesInput(
            ImmutableList.of(
                new StructuredPropertyInputParams(
                    TEST_PROPERTY_URN_1,
                    ImmutableList.of(new PropertyValueInput("new-value", null)))),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    verify(mockClient, times(1)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testGetSuccessMultipleProperties() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any())).thenReturn(true);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(TEST_ENTITY_URN_1));
    entityResponse.setEntityName("dataset");
    entityResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    BatchAddStructuredPropertiesResolver resolver =
        new BatchAddStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchAddStructuredPropertiesInput input =
        new BatchAddStructuredPropertiesInput(
            ImmutableList.of(
                new StructuredPropertyInputParams(
                    TEST_PROPERTY_URN_1, ImmutableList.of(new PropertyValueInput("value-1", null))),
                new StructuredPropertyInputParams(
                    TEST_PROPERTY_URN_2,
                    ImmutableList.of(new PropertyValueInput("value-2", null)))),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    verify(mockClient, times(1)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testGetFailurePropertyDoesNotExist() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any())).thenReturn(false);

    BatchAddStructuredPropertiesResolver resolver =
        new BatchAddStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchAddStructuredPropertiesInput input =
        new BatchAddStructuredPropertiesInput(
            ImmutableList.of(
                new StructuredPropertyInputParams(
                    TEST_PROPERTY_URN_1,
                    ImmutableList.of(new PropertyValueInput("test-value", null)))),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    verify(mockClient, times(0)).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    // Property exists but entity doesn't
    when(mockClient.exists(any(), eq(UrnUtils.getUrn(TEST_PROPERTY_URN_1)))).thenReturn(true);
    when(mockClient.exists(any(), eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)))).thenReturn(false);

    BatchAddStructuredPropertiesResolver resolver =
        new BatchAddStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchAddStructuredPropertiesInput input =
        new BatchAddStructuredPropertiesInput(
            ImmutableList.of(
                new StructuredPropertyInputParams(
                    TEST_PROPERTY_URN_1,
                    ImmutableList.of(new PropertyValueInput("test-value", null)))),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    verify(mockClient, times(0)).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testGetFailureUnauthorized() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any())).thenReturn(true);

    BatchAddStructuredPropertiesResolver resolver =
        new BatchAddStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchAddStructuredPropertiesInput input =
        new BatchAddStructuredPropertiesInput(
            ImmutableList.of(
                new StructuredPropertyInputParams(
                    TEST_PROPERTY_URN_1,
                    ImmutableList.of(new PropertyValueInput("test-value", null)))),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    verify(mockClient, times(0)).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testGetFailureSubResourceNotSupported() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any())).thenReturn(true);

    BatchAddStructuredPropertiesResolver resolver =
        new BatchAddStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchAddStructuredPropertiesInput input =
        new BatchAddStructuredPropertiesInput(
            ImmutableList.of(
                new StructuredPropertyInputParams(
                    TEST_PROPERTY_URN_1,
                    ImmutableList.of(new PropertyValueInput("test-value", null)))),
            ImmutableList.of(
                new ResourceRefInput(
                    TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "test-field")));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    verify(mockClient, times(0)).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testGetFailureRemoteInvocationException() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any()))
        .thenThrow(new RemoteInvocationException("Connection failed"));

    BatchAddStructuredPropertiesResolver resolver =
        new BatchAddStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchAddStructuredPropertiesInput input =
        new BatchAddStructuredPropertiesInput(
            ImmutableList.of(
                new StructuredPropertyInputParams(
                    TEST_PROPERTY_URN_1,
                    ImmutableList.of(new PropertyValueInput("test-value", null)))),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    verify(mockClient, times(0)).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any())).thenReturn(true);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(TEST_ENTITY_URN_1));
    entityResponse.setEntityName("dataset");
    entityResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    doThrow(RuntimeException.class).when(mockClient).ingestProposal(any(), any(), anyBoolean());

    BatchAddStructuredPropertiesResolver resolver =
        new BatchAddStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchAddStructuredPropertiesInput input =
        new BatchAddStructuredPropertiesInput(
            ImmutableList.of(
                new StructuredPropertyInputParams(
                    TEST_PROPERTY_URN_1,
                    ImmutableList.of(new PropertyValueInput("test-value", null)))),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetFailureEntityValidationRemoteInvocationException() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    // Make structured property validation pass
    when(mockClient.exists(any(), eq(UrnUtils.getUrn(TEST_PROPERTY_URN_1)))).thenReturn(true);

    // Make entity validation fail with RemoteInvocationException
    when(mockClient.exists(any(), eq(UrnUtils.getUrn(TEST_ENTITY_URN_1))))
        .thenThrow(new RemoteInvocationException("Failed to validate entity existence"));

    BatchAddStructuredPropertiesResolver resolver =
        new BatchAddStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchAddStructuredPropertiesInput input =
        new BatchAddStructuredPropertiesInput(
            ImmutableList.of(
                new StructuredPropertyInputParams(
                    TEST_PROPERTY_URN_1,
                    ImmutableList.of(new PropertyValueInput("test-value", null)))),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    verify(mockClient, times(0)).ingestProposal(any(), any(), anyBoolean());
  }
}
