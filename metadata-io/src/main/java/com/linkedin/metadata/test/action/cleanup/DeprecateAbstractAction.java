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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public abstract class DeprecateAbstractAction extends NoValidationAction {

    protected final EntityService entityService;

    protected MetadataChangeProposal getMetadataChangeProposal(
            Urn urn,
            boolean deprecate
    ) {
        String urnStr = urn.toString();
        Deprecation deprecation = DeprecationUtils.getDeprecation(
                entityService,
                urnStr,
                METADATA_TEST_ACTOR_URN,
                "Deprecated via metadata tests",
                deprecate,
                System.currentTimeMillis()
        );
        return TestUtils.buildProposalForUrn(
                urn,
                Constants.DEPRECATION_ASPECT_NAME,
                deprecation
        );
    }

    protected void ingestProposals(List<MetadataChangeProposal> proposals) {
        EntityUtils.ingestChangeProposals(
                proposals,
                entityService,
                METADATA_TEST_ACTOR_URN,
                true
        );
    }
}
