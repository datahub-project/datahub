package com.linkedin.metadata.test.action.api;

import com.linkedin.metadata.test.action.Action;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
/**
 * Actions that validate values field
 */
public abstract class ValuesAction implements Action {

    public static final String VALUES_PARAM = "values";

    @Override
    public void validate(ActionParameters params) throws InvalidActionParamsException {
        if (!params.getParams().containsKey(VALUES_PARAM)) {
            throw new InvalidActionParamsException("Action parameters are missing the required 'values' parameter.");
        }
    }
}