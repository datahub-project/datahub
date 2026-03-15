import { message } from 'antd';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ErrorCodes } from '@app/shared/constants';
import handleGraphQLError from '@app/shared/handleGraphQLError';

// Mock antd message
vi.mock('antd', () => ({
    message: {
        destroy: vi.fn(),
        error: vi.fn(),
    },
}));

describe('handleGraphQLError', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should show permission message for 403 Forbidden errors', () => {
        const error = {
            graphQLErrors: [{ message: 'Access denied', extensions: { code: ErrorCodes.Forbidden } }],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Default error',
        });

        expect(message.error).toHaveBeenCalledWith('Unauthorized. Please contact your DataHub administrator.');
    });

    it('should show server message for BadRequest (400) validation errors with errorSource=VALIDATION', () => {
        const validatorMessage = 'Cannot delete system tag: this tag is protected by policy';
        const error = {
            graphQLErrors: [{
                message: validatorMessage,
                extensions: { code: ErrorCodes.BadRequest, errorSource: 'VALIDATION' },
            }],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Something went wrong',
        });

        expect(message.error).toHaveBeenCalledWith(validatorMessage);
    });

    it('should fall back to badRequestMessage when server message is empty for validation errors', () => {
        const error = {
            graphQLErrors: [{
                message: '',
                extensions: { code: ErrorCodes.BadRequest, errorSource: 'VALIDATION' },
            }],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Default error',
            badRequestMessage: 'Bad request fallback',
        });

        expect(message.error).toHaveBeenCalledWith('Bad request fallback');
    });

    it('should use badRequestMessage for non-validation BadRequest errors (no errorSource)', () => {
        const error = {
            graphQLErrors: [{
                message: 'Some other bad request error',
                extensions: { code: ErrorCodes.BadRequest },
            }],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Default error',
            badRequestMessage: 'Bad request fallback',
        });

        expect(message.error).toHaveBeenCalledWith('Bad request fallback');
    });

    it('should use safe predefined message for ServerError (500), not expose server details', () => {
        const serverMessage = 'Database connection failed: no such index [datahub_usage_event_v1]';
        const error = {
            graphQLErrors: [{ message: serverMessage, extensions: { code: ErrorCodes.ServerError } }],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Default error',
            serverErrorMessage: 'Service temporarily unavailable',
        });

        // Should use serverErrorMessage, NOT the server's actual message (security)
        expect(message.error).toHaveBeenCalledWith('Service temporarily unavailable');
    });

    it('should show defaultMessage when no graphQLErrors present', () => {
        const error = {
            graphQLErrors: [],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Default error message',
        });

        expect(message.error).toHaveBeenCalledWith('Default error message');
    });

    it('should use defaultMessage for unknown error codes, not expose server details', () => {
        const serverMessage = 'Unknown internal error with sensitive details';
        const error = {
            graphQLErrors: [{ message: serverMessage, extensions: { code: 999 } }],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Default error',
        });

        // Should use defaultMessage, NOT the server's message (security)
        expect(message.error).toHaveBeenCalledWith('Default error');
    });

    it('should call message.destroy before showing error', () => {
        const error = {
            graphQLErrors: [{ message: 'Test error', extensions: { code: ErrorCodes.BadRequest } }],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Default error',
        });

        expect(message.destroy).toHaveBeenCalled();
        expect(message.error).toHaveBeenCalled();
    });
});
