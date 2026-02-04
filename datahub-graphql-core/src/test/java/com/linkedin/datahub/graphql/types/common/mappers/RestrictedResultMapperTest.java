package com.linkedin.datahub.graphql.types.common.mappers;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Restricted;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.services.RestrictedService;
import org.testng.annotations.Test;

public class RestrictedResultMapperTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,testDataset,PROD)";
  private static final String ENCRYPTED_URN = "urn:li:restricted:v2:encryptedValue123";

  @Test
  public void testCreateRestrictedResultWithService() {
    Urn datasetUrn = UrnUtils.getUrn(TEST_DATASET_URN);
    RestrictedService mockRestrictedService = mock(RestrictedService.class);
    Urn encryptedUrn = UrnUtils.getUrn(ENCRYPTED_URN);

    when(mockRestrictedService.encryptRestrictedUrn(datasetUrn)).thenReturn(encryptedUrn);

    DataFetcherResult<Entity> result =
        RestrictedResultMapper.createRestrictedResult(datasetUrn, mockRestrictedService);

    assertNotNull(result);
    assertNotNull(result.getData());
    assertTrue(result.getData() instanceof Restricted);

    Restricted restricted = (Restricted) result.getData();
    assertEquals(restricted.getType(), EntityType.RESTRICTED);
    assertEquals(restricted.getUrn(), ENCRYPTED_URN);

    verify(mockRestrictedService).encryptRestrictedUrn(datasetUrn);
  }

  @Test
  public void testCreateRestrictedResultWithoutService() {
    Urn datasetUrn = UrnUtils.getUrn(TEST_DATASET_URN);

    DataFetcherResult<Entity> result =
        RestrictedResultMapper.createRestrictedResult(datasetUrn, null);

    assertNotNull(result);
    assertNotNull(result.getData());
    assertTrue(result.getData() instanceof Restricted);

    Restricted restricted = (Restricted) result.getData();
    assertEquals(restricted.getType(), EntityType.RESTRICTED);
    // Without RestrictedService, original URN is used as fallback
    assertEquals(restricted.getUrn(), TEST_DATASET_URN);
  }
}
