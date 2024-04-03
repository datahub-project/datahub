package com.linkedin.datahub.graphql.utils;

import static com.linkedin.metadata.Constants.*;
import static org.testng.AssertJUnit.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.mxe.MetadataChangeProposal;
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
}
