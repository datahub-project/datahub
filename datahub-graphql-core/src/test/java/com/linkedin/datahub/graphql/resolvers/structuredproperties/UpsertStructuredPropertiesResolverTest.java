package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.PropertyValueInput;
import com.linkedin.datahub.graphql.generated.StringValue;
import com.linkedin.datahub.graphql.generated.StructuredPropertyInputParams;
import com.linkedin.datahub.graphql.generated.UpsertStructuredPropertiesInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import graphql.com.google.common.collect.ImmutableList;
import graphql.com.google.common.collect.ImmutableSet;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpsertStructuredPropertiesResolverTest {
  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)";
  private static final String PROPERTY_URN_1 = "urn:li:structuredProperty:test1";
  private static final String PROPERTY_URN_2 = "urn:li:structuredProperty:test2";

  private static final StructuredPropertyInputParams PROP_INPUT_1 =
      new StructuredPropertyInputParams(
          PROPERTY_URN_1, ImmutableList.of(new PropertyValueInput("test1", null)));
  private static final StructuredPropertyInputParams PROP_INPUT_2 =
      new StructuredPropertyInputParams(
          PROPERTY_URN_2, ImmutableList.of(new PropertyValueInput("test2", null)));
  private static final UpsertStructuredPropertiesInput TEST_INPUT =
      new UpsertStructuredPropertiesInput(
          TEST_DATASET_URN, ImmutableList.of(PROP_INPUT_1, PROP_INPUT_2));

  @Test
  public void testGetSuccessUpdateExisting() throws Exception {
    // mock it so that this entity already has values for the given two properties
    StructuredPropertyValueAssignmentArray initialProperties =
        new StructuredPropertyValueAssignmentArray();
    PrimitivePropertyValueArray propertyValues = new PrimitivePropertyValueArray();
    propertyValues.add(PrimitivePropertyValue.create("hello"));
    initialProperties.add(
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(UrnUtils.getUrn(PROPERTY_URN_1))
            .setValues(propertyValues));
    initialProperties.add(
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(UrnUtils.getUrn(PROPERTY_URN_2))
            .setValues(propertyValues));
    EntityClient mockEntityClient = initMockEntityClient(true, initialProperties);
    UpsertStructuredPropertiesResolver resolver =
        new UpsertStructuredPropertiesResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.StructuredProperties result =
        resolver.get(mockEnv).get();

    assertEquals(result.getProperties().size(), 2);
    assertEquals(result.getProperties().get(0).getStructuredProperty().getUrn(), PROPERTY_URN_1);
    assertEquals(result.getProperties().get(0).getValues().size(), 1);
    assertEquals(
        result.getProperties().get(0).getValues().get(0).toString(),
        new StringValue("test1").toString());
    assertEquals(result.getProperties().get(1).getStructuredProperty().getUrn(), PROPERTY_URN_2);
    assertEquals(result.getProperties().get(1).getValues().size(), 1);
    assertEquals(
        result.getProperties().get(1).getValues().get(0).toString(),
        new StringValue("test2").toString());

    // Validate that we called ingestProposal the correct number of times
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .ingestProposal(any(), Mockito.any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetSuccessNoExistingProps() throws Exception {
    // mock so the original entity has no structured properties
    EntityClient mockEntityClient = initMockEntityClient(true, null);
    UpsertStructuredPropertiesResolver resolver =
        new UpsertStructuredPropertiesResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.StructuredProperties result =
        resolver.get(mockEnv).get();

    assertEquals(result.getProperties().size(), 2);
    assertEquals(result.getProperties().get(0).getStructuredProperty().getUrn(), PROPERTY_URN_2);
    assertEquals(result.getProperties().get(0).getValues().size(), 1);
    assertEquals(
        result.getProperties().get(0).getValues().get(0).toString(),
        new StringValue("test2").toString());
    assertEquals(result.getProperties().get(1).getStructuredProperty().getUrn(), PROPERTY_URN_1);
    assertEquals(result.getProperties().get(1).getValues().size(), 1);
    assertEquals(
        result.getProperties().get(1).getValues().get(0).toString(),
        new StringValue("test1").toString());

    // Validate that we called ingestProposal the correct number of times
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .ingestProposal(any(), Mockito.any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetSuccessOneExistingOneNew() throws Exception {
    // mock so the original entity has one of the input props and one is new
    StructuredPropertyValueAssignmentArray initialProperties =
        new StructuredPropertyValueAssignmentArray();
    PrimitivePropertyValueArray propertyValues = new PrimitivePropertyValueArray();
    propertyValues.add(PrimitivePropertyValue.create("hello"));
    initialProperties.add(
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(UrnUtils.getUrn(PROPERTY_URN_1))
            .setValues(propertyValues));
    EntityClient mockEntityClient = initMockEntityClient(true, initialProperties);
    UpsertStructuredPropertiesResolver resolver =
        new UpsertStructuredPropertiesResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.StructuredProperties result =
        resolver.get(mockEnv).get();

    assertEquals(result.getProperties().size(), 2);
    assertEquals(result.getProperties().get(0).getStructuredProperty().getUrn(), PROPERTY_URN_1);
    assertEquals(result.getProperties().get(0).getValues().size(), 1);
    assertEquals(
        result.getProperties().get(0).getValues().get(0).toString(),
        new StringValue("test1").toString());
    assertEquals(result.getProperties().get(1).getStructuredProperty().getUrn(), PROPERTY_URN_2);
    assertEquals(result.getProperties().get(1).getValues().size(), 1);
    assertEquals(
        result.getProperties().get(1).getValues().get(0).toString(),
        new StringValue("test2").toString());

    // Validate that we called ingestProposal the correct number of times
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .ingestProposal(any(), Mockito.any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testThrowsError() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(false, null);
    UpsertStructuredPropertiesResolver resolver =
        new UpsertStructuredPropertiesResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we called ingestProposal the correct number of times
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .ingestProposal(any(), Mockito.any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  private EntityClient initMockEntityClient(
      final boolean shouldSucceed, @Nullable StructuredPropertyValueAssignmentArray properties)
      throws Exception {
    Urn assetUrn = UrnUtils.getUrn(TEST_DATASET_URN);
    EntityClient client = Mockito.mock(EntityClient.class);

    Mockito.when(client.exists(any(OperationContext.class), Mockito.eq(assetUrn), any()))
        .thenReturn(true);
    Mockito.when(client.exists(any(OperationContext.class), Mockito.eq(assetUrn))).thenReturn(true);

    if (!shouldSucceed) {
      Mockito.doThrow(new RuntimeException())
          .when(client)
          .getV2(any(), Mockito.any(), Mockito.any(), Mockito.any());
    } else {
      if (properties == null) {
        Mockito.when(
                client.getV2(
                    any(),
                    Mockito.eq(assetUrn.getEntityType()),
                    Mockito.eq(assetUrn),
                    Mockito.eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
            .thenReturn(null);
      } else {
        StructuredProperties structuredProps = new StructuredProperties();
        structuredProps.setProperties(properties);
        EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
        aspectMap.put(
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            new EnvelopedAspect().setValue(new Aspect(structuredProps.data())));
        EntityResponse response = new EntityResponse();
        response.setAspects(aspectMap);
        Mockito.when(
                client.getV2(
                    any(),
                    Mockito.eq(assetUrn.getEntityType()),
                    Mockito.eq(assetUrn),
                    Mockito.eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
            .thenReturn(response);
      }
    }
    return client;
  }
}
