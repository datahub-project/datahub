package com.linkedin.datahub.graphql.exception;

import graphql.ErrorClassification;
import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.GraphQLException;
import graphql.language.SourceLocation;

import java.util.List;

/**
 * Exception thrown when authorization to perform a particular action fails.
 */
public class AuthorizationError extends GraphQLException implements GraphQLError {

    private final String _message;

    public AuthorizationError(String message) {
        super(message);
        _message = message;
    }

    @Override
    public String getMessage() {
        return _message;
    }

    @Override
    public List<SourceLocation> getLocations() {
        return null;
    }

    @Override
    public ErrorClassification getErrorType() {
        return ErrorType.DataFetchingException;
    }
}
