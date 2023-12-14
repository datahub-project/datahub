package com.linkedin.metadata.test.action.domain;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.service.DomainService;
import com.linkedin.metadata.test.action.ActionType;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnsetDomainAction extends DomainAbstractAction {

  public UnsetDomainAction(DomainService domainService) {
    super(domainService);
  }

  @Override
  public ActionType getActionType() {
    return ActionType.UNSET_DOMAIN;
  }

  void applyInternal(Urn domainUrn, List<Urn> urns) {
    this.domainService.batchRemoveDomains(
        Arrays.asList(domainUrn), getResourceReferences(urns), METADATA_TESTS_SOURCE);
  }
}
