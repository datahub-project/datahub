package com.linkedin.datahub.upgrade.system.policies;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.core.io.Resource;

public class IngestPolicies implements BlockingSystemUpgrade {

  private final List<UpgradeStep> stepList;

  public IngestPolicies(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final EntitySearchService entitySearchService,
      @Nonnull final SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull final Resource policiesResource,
      final boolean enabled) {
    stepList =
        ImmutableList.of(
            new IngestPoliciesUpgradeStep(
                entityService,
                entitySearchService,
                searchDocumentTransformer,
                policiesResource,
                enabled));
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return stepList;
  }
}
