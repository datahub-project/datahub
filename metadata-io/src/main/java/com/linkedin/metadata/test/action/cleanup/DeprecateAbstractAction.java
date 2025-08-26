package com.linkedin.metadata.test.action.cleanup;

import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.utils.DeprecationUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.test.action.api.NoValidationAction;
import com.linkedin.metadata.test.util.TestUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public abstract class DeprecateAbstractAction extends NoValidationAction {

  protected final EntityService entityService;

  protected MetadataChangeProposal getMetadataChangeProposal(
      @Nonnull OperationContext opContext, Urn urn, boolean deprecate) {
    String urnStr = urn.toString();
    Deprecation deprecation =
        DeprecationUtils.getDeprecation(
            opContext,
            entityService,
            urnStr,
            METADATA_TEST_ACTOR_URN,
            "Deprecated via metadata tests",
            deprecate,
            System.currentTimeMillis(),
            null);
    return TestUtils.buildProposalForUrn(urn, Constants.DEPRECATION_ASPECT_NAME, deprecation);
  }

  protected void ingestProposals(
      @Nonnull OperationContext opContext, List<MetadataChangeProposal> proposals) {
    EntityUtils.ingestChangeProposals(
        opContext, proposals, entityService, METADATA_TEST_ACTOR_URN, true);
  }
}
