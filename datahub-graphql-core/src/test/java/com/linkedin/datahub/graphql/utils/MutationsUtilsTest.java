package com.linkedin.datahub.graphql.utils;

import static com.linkedin.datahub.graphql.TestUtils.verifyNoIngestProposal;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertThrows;
import static org.testng.AssertJUnit.*;

import com.linkedin.common.Status;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class MutationsUtilsTest {

  @Test
  public void testBuildMetadataChangeProposal() {
    MetadataChangeProposal metadataChangeProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            UrnUtils.getUrn("urn:li:corpuser:datahub"),
            CORP_USER_INFO_ASPECT_NAME,
            new CorpUserInfo().setActive(true));
    assertEquals(
        UI_SOURCE, metadataChangeProposal.getSystemMetadata().getProperties().get(APP_SOURCE));
    metadataChangeProposal =
        MutationUtils.buildMetadataChangeProposalWithKey(
            new CorpUserKey().setUsername("datahub"),
            CORP_USER_ENTITY_NAME,
            CORP_USER_INFO_ASPECT_NAME,
            new CorpUserInfo().setActive(true));
    assertEquals(
        UI_SOURCE, metadataChangeProposal.getSystemMetadata().getProperties().get(APP_SOURCE));
  }

  @Test
  public void testPersistAspectFailsWhenTheProposalIsRejected() {
    // An origin that is not a FabricType symbol can't be written; the mutation must not report
    // success for it.
    EntityService<?> entityService = mock(EntityService.class);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            MutationUtils.persistAspect(
                TestOperationContexts.systemContextNoSearchAuthorization(),
                UrnUtils.getUrn(
                    "urn:li:dataset:(urn:li:dataPlatform:hive,my_table,NOT_A_FABRIC_TYPE)"),
                STATUS_ASPECT_NAME,
                new Status().setRemoved(false),
                UrnUtils.getUrn("urn:li:corpuser:test"),
                entityService));
    verifyNoIngestProposal(entityService);
  }
}
