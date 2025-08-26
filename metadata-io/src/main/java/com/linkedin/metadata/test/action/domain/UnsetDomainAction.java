package com.linkedin.metadata.test.action.domain;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.service.DomainServiceAsync;
import com.linkedin.metadata.test.definition.ActionType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnsetDomainAction extends DomainAbstractAction {

  public UnsetDomainAction(DomainServiceAsync domainService) {
    super(domainService);
  }

  @Override
  public ActionType getActionType() {
    return ActionType.UNSET_DOMAIN;
  }

  void applyInternal(@Nonnull OperationContext opContext, Urn domainUrn, List<Urn> urns) {
    this.domainService.batchRemoveDomains(
        opContext, Arrays.asList(domainUrn), getResourceReferences(urns), METADATA_TESTS_SOURCE);
  }
}
