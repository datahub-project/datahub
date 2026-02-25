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

    it('should show server message for BadRequest (400) errors from validator plugins', () => {
        const validatorMessage = 'Cannot delete system tag: this tag is protected by policy';
        const error = {
            graphQLErrors: [{ message: validatorMessage, extensions: { code: ErrorCodes.BadRequest } }],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Something went wrong',
        });

        expect(message.error).toHaveBeenCalledWith(validatorMessage);
    });

    it('should fall back to badRequestMessage when server message is empty', () => {
        const error = {
            graphQLErrors: [{ message: '', extensions: { code: ErrorCodes.BadRequest } }],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Default error',
            badRequestMessage: 'Bad request fallback',
        });

        expect(message.error).toHaveBeenCalledWith('Bad request fallback');
    });

    it('should show server message for ServerError (500) when available', () => {
        const serverMessage = 'Database connection failed';
        const error = {
            graphQLErrors: [{ message: serverMessage, extensions: { code: ErrorCodes.ServerError } }],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Default error',
        });

        expect(message.error).toHaveBeenCalledWith(serverMessage);
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

    it('should use server message for unknown error codes', () => {
        const serverMessage = 'Unknown error occurred';
        const error = {
            graphQLErrors: [{ message: serverMessage, extensions: { code: 999 } }],
        };

        handleGraphQLError({
            error: error as any,
            defaultMessage: 'Default error',
        });

        expect(message.error).toHaveBeenCalledWith(serverMessage);
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
