package com.linkedin.metadata.test.action.cleanup;

import com.linkedin.common.urn.Urn;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.ActionType;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import lombok.extern.slf4j.Slf4j;


import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class DeprecationAction extends DeprecateAbstractAction {

    public DeprecationAction(EntityService entityService) {
        super(entityService);
    }

    @Override
    public ActionType getActionType() {
        return ActionType.DEPRECATE;
    }

    @Override
    public void apply(List<Urn> urns, ActionParameters params) throws InvalidOperandException {
        ingestProposals(
                urns.stream()
                        .map(urn -> getMetadataChangeProposal(urn, true))
                        .collect(Collectors.toList())
        );
    }
}