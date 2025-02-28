package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.RemoveStructuredPropertiesInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RemoveStructuredPropertiesResolverTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)";
  private static final String PROPERTY_URN_1 = "urn:li:structuredProperty:test1";
  private static final String PROPERTY_URN_2 = "urn:li:structuredProperty:test2";

  private static final RemoveStructuredPropertiesInput TEST_INPUT =
      new RemoveStructuredPropertiesInput(
          TEST_DATASET_URN, ImmutableList.of(PROPERTY_URN_1, PROPERTY_URN_2));

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient();
    RemoveStructuredPropertiesResolver resolver =
        new RemoveStructuredPropertiesResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    // Validate that we called ingest
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient();
    RemoveStructuredPropertiesResolver resolver =
        new RemoveStructuredPropertiesResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we did NOT call ingest
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .ingestProposal(any(), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetThrowsError() throws Exception {
    // if the entity you are trying to remove properties from doesn't exist
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockEntityClient.exists(any(), Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN))))
        .thenReturn(false);
    RemoveStructuredPropertiesResolver resolver =
        new RemoveStructuredPropertiesResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we did NOT call ingest
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .ingestProposal(any(), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  private EntityClient initMockEntityClient() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    EntityResponse response = new EntityResponse();
    response.setEntityName(Constants.DATASET_ENTITY_NAME);
    response.setUrn(UrnUtils.getUrn(TEST_DATASET_URN));
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    StructuredProperties properties = new StructuredProperties();
    properties.setProperties(new StructuredPropertyValueAssignmentArray());
    aspectMap.put(
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(properties.data())));
    response.setAspects(aspectMap);
    Mockito.when(
            client.getV2(
                any(),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(response);
    Mockito.when(client.exists(any(), Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN))))
        .thenReturn(true);

    return client;
  }
}
