package com.linkedin.metadata.test.action.domain;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.service.DomainServiceAsync;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SetDomainAction extends DomainAbstractAction {

  public SetDomainAction(DomainServiceAsync domainService) {
    super(domainService);
  }

  @Override
  public ActionType getActionType() {
    return ActionType.SET_DOMAIN;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    super.validate(params);
    List<String> domainUrns = params.getParams().get(VALUES_PARAM);
    if (domainUrns.size() != 1) {
      throw new InvalidActionParamsException(
          "Set domain action requires exactly one domain URN. Found: " + domainUrns.size());
    }
  }

  void applyInternal(@Nonnull OperationContext opContext, Urn domainUrn, List<Urn> urns) {
    this.domainService.batchSetDomain(
        opContext, domainUrn, getResourceReferences(urns), METADATA_TESTS_SOURCE);
  }
}
