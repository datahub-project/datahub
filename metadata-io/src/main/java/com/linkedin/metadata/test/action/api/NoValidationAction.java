package com.linkedin.metadata.test.action.api;

import com.linkedin.metadata.test.action.Action;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;

public abstract class NoValidationAction implements Action {

    @Override
    public void validate(ActionParameters params) throws InvalidActionParamsException {
        // Nothing to do as there are no params
    }
}
