package com.linkedin.metadata.test.action.cleanup;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnDeprecationAction extends DeprecateAbstractAction {

  public UnDeprecationAction(EntityService entityService) {
    super(entityService);
  }

  @Override
  public ActionType getActionType() {
    return ActionType.UN_DEPRECATE;
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    ingestProposals(
        opContext,
        urns.stream()
            .map(urn -> getMetadataChangeProposal(opContext, urn, false))
            .collect(Collectors.toList()));
  }
}
