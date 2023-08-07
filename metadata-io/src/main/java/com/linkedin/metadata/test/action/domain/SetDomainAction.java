package com.linkedin.metadata.test.action.domain;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.service.DomainService;
import com.linkedin.metadata.test.action.ActionType;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


@Slf4j
public class SetDomainAction extends DomainAbstractAction {

  public SetDomainAction(DomainService domainService) {
    super(domainService);
  }

  @Override
  public ActionType getActionType() {
    return ActionType.SET_DOMAIN;
  }

  void applyInternal(Urn domainUrn, List<Urn> urns) {
    this.domainService.batchSetDomain(domainUrn, getResourceReferences(urns), METADATA_TESTS_SOURCE);
  }
}