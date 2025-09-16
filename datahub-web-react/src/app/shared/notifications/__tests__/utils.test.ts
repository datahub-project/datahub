import {
    getErrorDisplayContentFromErrorCode,
    getErrorDisplayContentFromSlackErrorCode,
} from '@src/app/shared/notifications/utils';

describe('getErrorDisplayContentFromErrorCode', () => {
    const mockExtraContext = {
        destinationName: 'test-channel',
    };

    describe('Generic error patterns (our new functionality)', () => {
        it('should handle connection errors', () => {
            const result = getErrorDisplayContentFromErrorCode('connection failed', mockExtraContext);
            expect(result).toBe(
                'Unable to connect to the notification service. This could be a temporary network issue. Please try again, and if the problem persists, contact your DataHub admin.',
            );
        });

        it('should handle timeout errors', () => {
            const result = getErrorDisplayContentFromErrorCode('request timeout', mockExtraContext);
            expect(result).toBe(
                'The notification request timed out. Please try again, and if the problem persists, contact your DataHub admin.',
            );
        });

        it('should handle authentication errors', () => {
            const result = getErrorDisplayContentFromErrorCode('authentication failed', mockExtraContext);
            expect(result).toBe(
                'Authentication failed when sending the notification. The integration may need to be re-configured. Contact your DataHub admin.',
            );
        });

        it('should handle config errors', () => {
            const result = getErrorDisplayContentFromErrorCode('invalid config', mockExtraContext);
            expect(result).toBe(
                'The notification service configuration appears to be invalid. Contact your DataHub admin to verify the integration settings.',
            );
        });

        it('should handle not found errors with destination name', () => {
            const result = getErrorDisplayContentFromErrorCode('not found', mockExtraContext);
            expect(result).toBe(
                "The destination 'test-channel' was not found. Please verify the destination exists and is accessible.",
            );
        });

        it('should handle RuntimeException', () => {
            const result = getErrorDisplayContentFromErrorCode('RuntimeException', mockExtraContext);
            expect(result).toBe(
                'An unexpected error occurred while sending the notification. Please try again, and if the problem persists, contact your DataHub admin.',
            );
        });
    });

    describe('Existing Slack-specific error handling (unchanged)', () => {
        it('should handle channel_not_found error', () => {
            const result = getErrorDisplayContentFromErrorCode('channel_not_found', mockExtraContext);
            expect(result).toContain("no channel with the name 'test-channel'");
        });

        it('should handle not_in_channel error', () => {
            const result = getErrorDisplayContentFromErrorCode('not_in_channel', mockExtraContext);
            expect(result).toContain('add the DataHub Slackbot into test-channel');
        });

        it('should handle is_archived error', () => {
            const result = getErrorDisplayContentFromErrorCode('is_archived', mockExtraContext);
            expect(result).toContain('channel has been archived');
        });
    });

    describe('Default behavior', () => {
        it('should handle unknown error codes with generic message', () => {
            const result = getErrorDisplayContentFromErrorCode('unknown_error_code', mockExtraContext);
            expect(result).toBe("Notification failed to send with error 'unknown_error_code'");
        });

        it('should handle empty error codes', () => {
            const result = getErrorDisplayContentFromErrorCode('', mockExtraContext);
            expect(result).toBe("Notification failed to send with error ''");
        });
    });

    describe('Priority handling (generic patterns vs specific Slack codes)', () => {
        it('should prefer specific Slack error over generic pattern', () => {
            // "not_in_channel" contains "not" which could match "not found" pattern,
            // but should use specific Slack handling instead
            const result = getErrorDisplayContentFromErrorCode('not_in_channel', mockExtraContext);
            expect(result).toContain('add the DataHub Slackbot');
            expect(result).not.toContain('not found');
        });
    });

    describe('Backwards compatibility', () => {
        it('should export getErrorDisplayContentFromSlackErrorCode', () => {
            expect(getErrorDisplayContentFromSlackErrorCode).toBeDefined();
            expect(typeof getErrorDisplayContentFromSlackErrorCode).toBe('function');
        });

        it('should have same behavior for backwards compatibility function', () => {
            const errorCode = 'channel_not_found';
            const result1 = getErrorDisplayContentFromErrorCode(errorCode, mockExtraContext);
            const result2 = getErrorDisplayContentFromSlackErrorCode(errorCode, mockExtraContext);
            expect(result1).toBe(result2);
        });
    });
});
