package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchRemoveStructuredPropertiesInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
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

public class BatchRemoveStructuredPropertiesResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_PROPERTY_URN_1 = "urn:li:structuredProperty:test-property-1";
  private static final String TEST_PROPERTY_URN_2 = "urn:li:structuredProperty:test-property-2";

  @Test
  public void testGetSuccessRemoveProperties() throws Exception {
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

    BatchRemoveStructuredPropertiesResolver resolver =
        new BatchRemoveStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchRemoveStructuredPropertiesInput input =
        new BatchRemoveStructuredPropertiesInput(
            ImmutableList.of(TEST_PROPERTY_URN_1),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    verify(mockClient, times(2)).getV2(any(), any(), any(), any());
    verify(mockClient, times(0)).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testGetSuccessRemoveExistingProperties() throws Exception {
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
    StructuredPropertyValueAssignment assignment1 = new StructuredPropertyValueAssignment();
    assignment1.setPropertyUrn(UrnUtils.getUrn(TEST_PROPERTY_URN_1));
    StructuredPropertyValueAssignment assignment2 = new StructuredPropertyValueAssignment();
    assignment2.setPropertyUrn(UrnUtils.getUrn(TEST_PROPERTY_URN_2));

    existingProps.setProperties(
        new StructuredPropertyValueAssignmentArray(ImmutableList.of(assignment1, assignment2)));

    structuredPropsAspect.setValue(new Aspect(existingProps.data()));
    aspectMap.put("structuredProperties", structuredPropsAspect);
    entityResponse.setAspects(aspectMap);

    when(mockClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    BatchRemoveStructuredPropertiesResolver resolver =
        new BatchRemoveStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchRemoveStructuredPropertiesInput input =
        new BatchRemoveStructuredPropertiesInput(
            ImmutableList.of(TEST_PROPERTY_URN_1),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    // Should ingest proposal since we're removing an existing property
    verify(mockClient, times(1)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testGetSuccessRemoveMultipleProperties() throws Exception {
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
    StructuredPropertyValueAssignment assignment1 = new StructuredPropertyValueAssignment();
    assignment1.setPropertyUrn(UrnUtils.getUrn(TEST_PROPERTY_URN_1));
    StructuredPropertyValueAssignment assignment2 = new StructuredPropertyValueAssignment();
    assignment2.setPropertyUrn(UrnUtils.getUrn(TEST_PROPERTY_URN_2));

    existingProps.setProperties(
        new StructuredPropertyValueAssignmentArray(ImmutableList.of(assignment1, assignment2)));

    structuredPropsAspect.setValue(new Aspect(existingProps.data()));
    aspectMap.put("structuredProperties", structuredPropsAspect);
    entityResponse.setAspects(aspectMap);

    when(mockClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    BatchRemoveStructuredPropertiesResolver resolver =
        new BatchRemoveStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchRemoveStructuredPropertiesInput input =
        new BatchRemoveStructuredPropertiesInput(
            ImmutableList.of(TEST_PROPERTY_URN_1, TEST_PROPERTY_URN_2),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    // Should ingest proposal since we're removing existing properties
    verify(mockClient, times(1)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testGetSuccessRemoveNonExistentProperty() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any())).thenReturn(true);

    // Create entity response with existing structured properties
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(TEST_ENTITY_URN_1));
    entityResponse.setEntityName("dataset");

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect structuredPropsAspect = new EnvelopedAspect();

    // Create existing structured properties (only property 2)
    StructuredProperties existingProps = new StructuredProperties();
    StructuredPropertyValueAssignment assignment2 = new StructuredPropertyValueAssignment();
    assignment2.setPropertyUrn(UrnUtils.getUrn(TEST_PROPERTY_URN_2));

    existingProps.setProperties(
        new StructuredPropertyValueAssignmentArray(ImmutableList.of(assignment2)));

    structuredPropsAspect.setValue(new Aspect(existingProps.data()));
    aspectMap.put("structuredProperties", structuredPropsAspect);
    entityResponse.setAspects(aspectMap);

    when(mockClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    BatchRemoveStructuredPropertiesResolver resolver =
        new BatchRemoveStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchRemoveStructuredPropertiesInput input =
        new BatchRemoveStructuredPropertiesInput(
            ImmutableList.of(TEST_PROPERTY_URN_1), // Property 1 doesn't exist
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    // Should not ingest proposal since the property doesn't exist
    verify(mockClient, times(0)).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any())).thenReturn(false);

    BatchRemoveStructuredPropertiesResolver resolver =
        new BatchRemoveStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchRemoveStructuredPropertiesInput input =
        new BatchRemoveStructuredPropertiesInput(
            ImmutableList.of(TEST_PROPERTY_URN_1),
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

    BatchRemoveStructuredPropertiesResolver resolver =
        new BatchRemoveStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchRemoveStructuredPropertiesInput input =
        new BatchRemoveStructuredPropertiesInput(
            ImmutableList.of(TEST_PROPERTY_URN_1),
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

    BatchRemoveStructuredPropertiesResolver resolver =
        new BatchRemoveStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchRemoveStructuredPropertiesInput input =
        new BatchRemoveStructuredPropertiesInput(
            ImmutableList.of(TEST_PROPERTY_URN_1),
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

    BatchRemoveStructuredPropertiesResolver resolver =
        new BatchRemoveStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchRemoveStructuredPropertiesInput input =
        new BatchRemoveStructuredPropertiesInput(
            ImmutableList.of(TEST_PROPERTY_URN_1),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    verify(mockClient, times(0)).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    // Mock all entities exist
    when(mockClient.exists(any(), any())).thenReturn(true);

    // Mock empty structured properties response
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(TEST_ENTITY_URN_1));
    entityResponse.setEntityName("dataset");
    entityResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    // Mock ingestProposal to throw exception
    doThrow(RuntimeException.class).when(mockClient).ingestProposal(any(), any(), anyBoolean());

    BatchRemoveStructuredPropertiesResolver resolver =
        new BatchRemoveStructuredPropertiesResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchRemoveStructuredPropertiesInput input =
        new BatchRemoveStructuredPropertiesInput(
            ImmutableList.of(TEST_PROPERTY_URN_1),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    // This should succeed because there are no existing properties to remove,
    // so ingestProposal is never called
    assertTrue(resolver.get(mockEnv).get());
  }

  @Test
  public void testGetFailureBatchRemoveStructuredPropertiesException() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any())).thenReturn(true);

    // Create entity response with existing structured properties to trigger ingestProposal
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(TEST_ENTITY_URN_1));
    entityResponse.setEntityName("dataset");

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect structuredPropsAspect = new EnvelopedAspect();

    // Create existing structured properties
    StructuredProperties existingProps = new StructuredProperties();
    StructuredPropertyValueAssignment assignment1 = new StructuredPropertyValueAssignment();
    assignment1.setPropertyUrn(UrnUtils.getUrn(TEST_PROPERTY_URN_1));

    existingProps.setProperties(
        new StructuredPropertyValueAssignmentArray(ImmutableList.of(assignment1)));

    structuredPropsAspect.setValue(new Aspect(existingProps.data()));
    aspectMap.put("structuredProperties", structuredPropsAspect);
    entityResponse.setAspects(aspectMap);

    when(mockClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    // Mock ingestProposal to throw exception to trigger the exception path
    doThrow(new RuntimeException("Ingestion failed"))
        .when(mockClient)
        .ingestProposal(any(), any(), anyBoolean());

    BatchRemoveStructuredPropertiesResolver resolver =
        new BatchRemoveStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchRemoveStructuredPropertiesInput input =
        new BatchRemoveStructuredPropertiesInput(
            ImmutableList.of(TEST_PROPERTY_URN_1),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    // This should throw a CompletionException due to the underlying RuntimeException
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetFailureBatchRemoveStructuredPropertiesGetV2Exception() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    when(mockClient.exists(any(), any())).thenReturn(true);

    // Mock getV2 to throw exception
    when(mockClient.getV2(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Failed to get entity"));

    BatchRemoveStructuredPropertiesResolver resolver =
        new BatchRemoveStructuredPropertiesResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    BatchRemoveStructuredPropertiesInput input =
        new BatchRemoveStructuredPropertiesInput(
            ImmutableList.of(TEST_PROPERTY_URN_1),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    // This should throw a CompletionException due to the underlying RuntimeException
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
