package com.linkedin.metadata.test.action.cleanup;

import com.linkedin.common.urn.Urn;

import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.ActionType;
import com.linkedin.metadata.test.action.api.NoValidationAction;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class DeprecationAction extends NoValidationAction {
    @Override
    public ActionType getActionType() {
        return ActionType.DEPRECATION;
    }

    @Override
    public void apply(List<Urn> urns, ActionParameters params) throws InvalidOperandException {
        throw new InvalidOperandException("Not implemented yet");
    }
}