package com.linkedin.datahub.graphql.resolvers.mutate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;

import org.mockito.ArgumentCaptor;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.dataset.EditableDatasetProperties;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DescriptionUtilsTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testUser";
  private static final String TEST_DESCRIPTION = "This is a test description for the dataset";

  private EntityService<?> mockEntityService;
  private OperationContext mockOpContext;

  @BeforeMethod
  public void setup() {
    mockEntityService = Mockito.mock(EntityService.class);
    mockOpContext = Mockito.mock(OperationContext.class);
  }

  @Test
  public void testUpdateDatasetDescriptionWithValidString() throws Exception {
    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    Urn actorUrn = Urn.createFromString(TEST_ACTOR_URN);

    DescriptionUtils.updateDatasetDescription(
        mockOpContext, TEST_DESCRIPTION, datasetUrn, actorUrn, mockEntityService);

    ArgumentCaptor<MetadataChangeProposal> captor = ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityService).ingestProposal(eq(mockOpContext), captor.capture(), any(), eq(false));

    MetadataChangeProposal proposal = captor.getValue();
    EditableDatasetProperties props = new EditableDatasetProperties(proposal.getAspect().data());
    
    assertTrue(props.hasDescription());
    assertEquals(props.getDescription(), TEST_DESCRIPTION);
  }

  @Test
  public void testUpdateDatasetDescriptionWithEmptyString() throws Exception {
    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    Urn actorUrn = Urn.createFromString(TEST_ACTOR_URN);

    DescriptionUtils.updateDatasetDescription(
        mockOpContext, "   ", datasetUrn, actorUrn, mockEntityService);

    ArgumentCaptor<MetadataChangeProposal> captor = ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityService).ingestProposal(eq(mockOpContext), captor.capture(), any(), eq(false));

    MetadataChangeProposal proposal = captor.getValue();
    EditableDatasetProperties props = new EditableDatasetProperties(proposal.getAspect().data());
    
    assertFalse(props.hasDescription(), "Description should be removed when empty string is provided");
  }

  @Test
  public void testValidateDescriptionInputSuccess() throws Exception {
    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);

    Mockito.when(mockEntityService.exists(mockOpContext, datasetUrn, true)).thenReturn(true);

    Boolean result =
        DescriptionUtils.validateDescriptionInput(mockOpContext, datasetUrn, mockEntityService);

    assertTrue(result);
    verify(mockEntityService).exists(mockOpContext, datasetUrn, true);
  }

  @Test
  public void testValidateDescriptionInputFailure() throws Exception {
    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);

    Mockito.when(mockEntityService.exists(mockOpContext, datasetUrn, true)).thenReturn(false);

    try {
      DescriptionUtils.validateDescriptionInput(mockOpContext, datasetUrn, mockEntityService);
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException exception) {
      assertTrue(exception.getMessage().contains("does not exist"));
      assertTrue(exception.getMessage().contains(TEST_DATASET_URN));
    }

    verify(mockEntityService).exists(mockOpContext, datasetUrn, true);
  }
}
