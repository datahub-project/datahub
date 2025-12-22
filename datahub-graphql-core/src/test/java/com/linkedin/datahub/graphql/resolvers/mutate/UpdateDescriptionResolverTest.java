package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DescriptionUpdateInput;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateDescriptionResolverTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final String TEST_DESCRIPTION = "Updated test description";

  private EntityService<?> mockEntityService;
  private EntityClient mockEntityClient;
  private UpdateDescriptionResolver resolver;

  @BeforeMethod
  public void setup() {
    mockEntityService = getMockEntityService();
    mockEntityClient = Mockito.mock(EntityClient.class);
    resolver = new UpdateDescriptionResolver(mockEntityService, mockEntityClient);
  }

  @Test
  public void testUpdateDatasetTopLevelDescription() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DATASET_URN)), eq(true)))
        .thenReturn(true);

    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DATASET_URN);
    input.setDescription(TEST_DESCRIPTION);
    // No subResourceType - should update top-level description

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    EditableDatasetProperties expectedProperties = new EditableDatasetProperties();
    expectedProperties.setDescription(TEST_DESCRIPTION);

    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_DATASET_URN),
            EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
            expectedProperties);

    verifySingleIngestProposal(mockEntityService, 1, proposal);
  }

  @Test
  public void testUpdateDatasetTopLevelDescriptionUnauthorized() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DATASET_URN)), eq(true)))
        .thenReturn(true);

    QueryContext mockContext = getMockDenyContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DATASET_URN);
    input.setDescription(TEST_DESCRIPTION);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(Exception.class, () -> resolver.get(mockEnv).get());

    Mockito.verify(mockEntityService, Mockito.never())
        .ingestProposal(any(), any(), any(), eq(false));
  }

  @Test
  public void testUpdateDatasetDescriptionNonExistentDataset() throws Exception {
    Mockito.when(
            mockEntityService.exists(any(), eq(Urn.createFromString(TEST_DATASET_URN)), eq(true)))
        .thenReturn(false);

    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(TEST_DATASET_URN);
    input.setDescription(TEST_DESCRIPTION);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(Exception.class, () -> resolver.get(mockEnv).get());

    Mockito.verify(mockEntityService, Mockito.never())
        .ingestProposal(any(), any(), any(), eq(false));
  }
}
