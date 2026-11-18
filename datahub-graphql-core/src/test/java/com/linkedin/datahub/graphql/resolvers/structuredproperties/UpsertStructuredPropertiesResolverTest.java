package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockContextDenyingSubResource;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.MetadataAttribution;
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
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationUtils;
import com.linkedin.metadata.aspect.patch.template.common.GenericPatchTemplate;
import com.linkedin.metadata.aspect.patch.template.common.StructuredPropertiesTemplate;
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
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpsertStructuredPropertiesResolverTest {
  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)";
  private static final String PROPERTY_URN_1 = "urn:li:structuredProperty:test1";
  private static final String PROPERTY_URN_2 = "urn:li:structuredProperty:test2";
  private static final String PROPERTY_URN_3 = "urn:li:structuredProperty:test3";
  private static final StructuredPropertyInputParams PROP_INPUT_1 =
      new StructuredPropertyInputParams(
          PROPERTY_URN_1, ImmutableList.of(new PropertyValueInput("test1", null)));
  private static final StructuredPropertyInputParams PROP_INPUT_2 =
      new StructuredPropertyInputParams(
          PROPERTY_URN_2, ImmutableList.of(new PropertyValueInput("test2", null)));
  private static final UpsertStructuredPropertiesInput TEST_INPUT =
      new UpsertStructuredPropertiesInput(
          TEST_DATASET_URN, ImmutableList.of(PROP_INPUT_1, PROP_INPUT_2));

  /**
   * Another writer sets a property after the resolver read the aspect and before its write lands.
   * The resolver sends a patch, which GMS applies to the latest aspect, so that value is kept. The
   * previous whole-aspect upsert, built from the earlier read, overwrote it.
   */
  @Test
  public void testConcurrentWriteToAnotherPropertyIsKept() throws Exception {
    StructuredProperties readByResolver = properties(assignment(PROPERTY_URN_1, "hello"));
    EntityClient mockEntityClient = initMockEntityClient(true, readByResolver, null);

    executeResolver(mockEntityClient, getMockAllowContext());

    MetadataChangeProposal proposal = captureProposal(mockEntityClient);
    assertEquals(proposal.getChangeType(), ChangeType.PATCH);
    assertEquals(proposal.getAspectName(), STRUCTURED_PROPERTIES_ASPECT_NAME);
    assertEquals(proposal.getEntityUrn().toString(), TEST_DATASET_URN);

    StructuredProperties latest =
        properties(assignment(PROPERTY_URN_1, "hello"), assignment(PROPERTY_URN_3, "concurrent"));
    Map<String, StructuredPropertyValueAssignment> stored =
        byProperty(applyPatch(proposal, latest));

    assertEquals(stored.size(), 3);
    assertEquals(stringValue(stored.get(PROPERTY_URN_1)), "test1");
    assertEquals(stringValue(stored.get(PROPERTY_URN_2)), "test2");
    assertEquals(stringValue(stored.get(PROPERTY_URN_3)), "concurrent");
    assertEquals(
        stored.get(PROPERTY_URN_1).getLastModified().getActor().toString(), "urn:li:corpuser:test");
  }

  @Test
  public void testWritesToAnEntityWithoutStructuredProperties() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true, null, null);

    executeResolver(mockEntityClient, getMockAllowContext());

    Map<String, StructuredPropertyValueAssignment> stored =
        byProperty(applyPatch(captureProposal(mockEntityClient), null));
    assertEquals(stored.size(), 2);
    assertEquals(stringValue(stored.get(PROPERTY_URN_1)), "test1");
    assertEquals(stringValue(stored.get(PROPERTY_URN_2)), "test2");
  }

  /**
   * A manual value takes over values that an attributed source (for example propagation) wrote for
   * the same property: one assignment remains, without attribution.
   */
  @Test
  public void testManualValueReplacesAttributedValues() throws Exception {
    StructuredPropertyValueAssignment propagated =
        assignment(PROPERTY_URN_1, "hello")
            .setAttribution(
                new MetadataAttribution()
                    .setTime(1234567890L)
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:testuser"))
                    .setSource(UrnUtils.getUrn("urn:li:dataHubAction:propagation")));
    StructuredProperties existing = properties(propagated, assignment(PROPERTY_URN_2, "hello"));
    EntityClient mockEntityClient = initMockEntityClient(true, existing, null);

    executeResolver(mockEntityClient, getMockAllowContext());

    StructuredProperties stored = applyPatch(captureProposal(mockEntityClient), existing);
    assertEquals(stored.getProperties().size(), 2);
    Map<String, StructuredPropertyValueAssignment> byUrn = byProperty(stored);
    assertEquals(stringValue(byUrn.get(PROPERTY_URN_1)), "test1");
    assertFalse(byUrn.get(PROPERTY_URN_1).hasAttribution());
    assertEquals(stringValue(byUrn.get(PROPERTY_URN_2)), "test2");
  }

  @Test
  public void testReturnsThePropertiesReadAfterTheWrite() throws Exception {
    StructuredProperties storedAfterWrite =
        properties(
            assignment(PROPERTY_URN_1, "test1"),
            assignment(PROPERTY_URN_2, "test2"),
            assignment(PROPERTY_URN_3, "concurrent"));
    EntityClient mockEntityClient = initMockEntityClient(true, null, storedAfterWrite);

    com.linkedin.datahub.graphql.generated.StructuredProperties result =
        executeResolver(mockEntityClient, getMockAllowContext());

    assertEquals(result.getProperties().size(), 3);
    assertEquals(result.getProperties().get(2).getStructuredProperty().getUrn(), PROPERTY_URN_3);
    assertEquals(
        result.getProperties().get(2).getValues().get(0).toString(),
        new StringValue("concurrent").toString());

    InOrder inOrder = Mockito.inOrder(mockEntityClient);
    inOrder.verify(mockEntityClient).getV2(any(), any(), any(), any());
    inOrder
        .verify(mockEntityClient)
        .ingestProposal(any(), any(MetadataChangeProposal.class), Mockito.eq(false));
    inOrder.verify(mockEntityClient).getV2(any(), any(), any(), any());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true, null, null);
    UpsertStructuredPropertiesResolver resolver =
        new UpsertStructuredPropertiesResolver(mockEntityClient);

    // Execute resolver with a context that denies all privileges
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we never attempted to ingest the change
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .ingestProposal(any(), Mockito.any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  // getMockDenyContext (used by testGetUnauthorized) denies every privilege outright, so it
  // would pass even if the resolver never forwarded the structured property urns for scoped
  // authorization. This proves a per-property constraint - entity edit allowed, one property
  // denied - actually blocks the write.
  @Test
  public void testGetDeniedWhenStructuredPropertyConstrained() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true, null, null);
    UpsertStructuredPropertiesResolver resolver =
        new UpsertStructuredPropertiesResolver(mockEntityClient);

    QueryContext mockContext =
        getMockContextDenyingSubResource("urn:li:corpuser:test", UrnUtils.getUrn(PROPERTY_URN_1));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we never attempted to ingest the change
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .ingestProposal(any(), Mockito.any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testThrowsError() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(false, null, null);
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

  private static com.linkedin.datahub.graphql.generated.StructuredProperties executeResolver(
      EntityClient mockEntityClient, QueryContext mockContext) throws Exception {
    UpsertStructuredPropertiesResolver resolver =
        new UpsertStructuredPropertiesResolver(mockEntityClient);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    return resolver.get(mockEnv).get();
  }

  private static MetadataChangeProposal captureProposal(EntityClient mockEntityClient)
      throws Exception {
    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .ingestProposal(any(), captor.capture(), Mockito.eq(false));
    return captor.getValue();
  }

  /** Applies a patch proposal the way GMS does for an aspect patched with array primary keys. */
  private static StructuredProperties applyPatch(
      MetadataChangeProposal proposal, @Nullable StructuredProperties current) throws Exception {
    GenericJsonPatch patch =
        PatchOperationUtils.convertToJsonPatch(proposal).getSecond().orElseThrow();
    StructuredProperties templateDefault = new StructuredPropertiesTemplate().getDefault();
    return GenericPatchTemplate.<StructuredProperties>builder()
        .genericJsonPatch(patch)
        .templateType(StructuredProperties.class)
        .templateDefault(templateDefault)
        .build()
        .applyPatch(current != null ? current : templateDefault);
  }

  private static StructuredPropertyValueAssignment assignment(String propertyUrn, String value) {
    return new StructuredPropertyValueAssignment()
        .setPropertyUrn(UrnUtils.getUrn(propertyUrn))
        .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(value)));
  }

  private static StructuredProperties properties(StructuredPropertyValueAssignment... assignments) {
    return new StructuredProperties()
        .setProperties(new StructuredPropertyValueAssignmentArray(Arrays.asList(assignments)));
  }

  private static Map<String, StructuredPropertyValueAssignment> byProperty(
      StructuredProperties structuredProperties) {
    return structuredProperties.getProperties().stream()
        .collect(Collectors.toMap(a -> a.getPropertyUrn().toString(), a -> a));
  }

  private static String stringValue(StructuredPropertyValueAssignment assignment) {
    return assignment.getValues().get(0).getString();
  }

  /**
   * @param readBeforeWrite the aspect the resolver reads before writing, or null if the entity has
   *     none
   * @param readAfterWrite the aspect the resolver reads back after writing; defaults to
   *     readBeforeWrite
   */
  private EntityClient initMockEntityClient(
      final boolean shouldSucceed,
      @Nullable StructuredProperties readBeforeWrite,
      @Nullable StructuredProperties readAfterWrite)
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
      Mockito.when(
              client.getV2(
                  any(),
                  Mockito.eq(assetUrn.getEntityType()),
                  Mockito.eq(assetUrn),
                  Mockito.eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
          .thenReturn(
              entityResponse(readBeforeWrite),
              entityResponse(readAfterWrite != null ? readAfterWrite : readBeforeWrite));
    }
    return client;
  }

  @Nullable
  private static EntityResponse entityResponse(@Nullable StructuredProperties structuredProps) {
    if (structuredProps == null) {
      return null;
    }
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(structuredProps.data())));
    EntityResponse response = new EntityResponse();
    response.setAspects(aspectMap);
    return response;
  }
}
