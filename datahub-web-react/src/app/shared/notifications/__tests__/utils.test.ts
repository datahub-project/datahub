import { getDestinationId, getErrorDisplayContentFromErrorCode } from '@app/shared/notifications/utils';

describe('getDestinationId', () => {
    describe('Slack integration', () => {
        it('returns channel names when channels are provided', () => {
            const result = getDestinationId({
                integration: 'slack',
                destinationSettings: {
                    channels: ['#general', '#random'],
                },
            });
            expect(result).toBe('#general, #random');
        });

        it('returns OAuth user displayName when user object is provided', () => {
            const result = getDestinationId({
                integration: 'slack',
                destinationSettings: {
                    user: {
                        slackUserId: 'U12345678',
                        displayName: 'John Doe',
                    },
                },
            });
            expect(result).toBe('John Doe');
        });

        it('falls back to slackUserId when displayName is not available', () => {
            const result = getDestinationId({
                integration: 'slack',
                destinationSettings: {
                    user: {
                        slackUserId: 'U12345678',
                    },
                },
            });
            expect(result).toBe('U12345678');
        });

        it('falls back to legacy userHandle when user object is not available', () => {
            const result = getDestinationId({
                integration: 'slack',
                destinationSettings: {
                    userHandle: 'U87654321',
                },
            });
            expect(result).toBe('U87654321');
        });

        it('falls back to userHandle in user object when user fields are empty', () => {
            const result = getDestinationId({
                integration: 'slack',
                destinationSettings: {
                    user: {
                        slackUserId: null,
                        displayName: null,
                    },
                    userHandle: 'U87654321',
                },
            });
            expect(result).toBe('U87654321');
        });

        it('returns "Slack user" when no identifiable info is available', () => {
            const result = getDestinationId({
                integration: 'slack',
                destinationSettings: {
                    user: {},
                },
            });
            expect(result).toBe('Slack user');
        });

        it('returns "no destination specified" when no settings are provided', () => {
            const result = getDestinationId({
                integration: 'slack',
                destinationSettings: {},
            });
            expect(result).toBe('no destination specified');
        });

        it('prefers channels over user settings', () => {
            const result = getDestinationId({
                integration: 'slack',
                destinationSettings: {
                    channels: ['#general'],
                    user: {
                        displayName: 'John Doe',
                    },
                },
            });
            expect(result).toBe('#general');
        });
    });

    describe('Teams integration', () => {
        it('returns channel names when channels are provided', () => {
            const result = getDestinationId({
                integration: 'teams',
                destinationSettings: {
                    channels: [
                        { id: '1', name: 'General' },
                        { id: '2', name: 'Random' },
                    ],
                },
            });
            expect(result).toBe('General, Random');
        });

        it('returns user displayName when user object is provided', () => {
            const result = getDestinationId({
                integration: 'teams',
                destinationSettings: {
                    user: {
                        displayName: 'Jane Doe',
                        azureUserId: 'azure-123',
                        email: 'jane@example.com',
                    },
                },
            });
            expect(result).toBe('Jane Doe');
        });

        it('falls back to azureUserId when displayName is not available', () => {
            const result = getDestinationId({
                integration: 'teams',
                destinationSettings: {
                    user: {
                        azureUserId: 'azure-123',
                        email: 'jane@example.com',
                    },
                },
            });
            expect(result).toBe('azure-123');
        });

        it('falls back to email when neither displayName nor azureUserId is available', () => {
            const result = getDestinationId({
                integration: 'teams',
                destinationSettings: {
                    user: {
                        email: 'jane@example.com',
                    },
                },
            });
            expect(result).toBe('jane@example.com');
        });

        it('returns "Teams user" when no user fields are available', () => {
            const result = getDestinationId({
                integration: 'teams',
                destinationSettings: {
                    user: {},
                },
            });
            expect(result).toBe('Teams user');
        });
    });
});

describe('getErrorDisplayContentFromErrorCode', () => {
    const extraContext = { destinationName: '#test-channel' };

    it('returns specific message for channel_not_found error', () => {
        const result = getErrorDisplayContentFromErrorCode('channel_not_found', extraContext);
        expect(result).toContain('#test-channel');
        expect(result).toContain('no channel with the name');
    });

    it('returns specific message for not_in_channel error', () => {
        const result = getErrorDisplayContentFromErrorCode('not_in_channel', extraContext);
        expect(result).toContain('#test-channel');
        expect(result).toContain('add the DataHub Slackbot');
    });

    it('returns specific message for token_expired error', () => {
        const result = getErrorDisplayContentFromErrorCode('token_expired', extraContext);
        expect(result).toContain('token is expired');
    });

    it('returns generic message for unknown error codes', () => {
        const result = getErrorDisplayContentFromErrorCode('some_unknown_error', extraContext);
        expect(result).toContain("error 'some_unknown_error'");
    });

    it('handles connection-related errors', () => {
        const result = getErrorDisplayContentFromErrorCode('connection_failed', extraContext);
        expect(result).toContain('Unable to connect');
    });

    it('handles authentication-related errors', () => {
        const result = getErrorDisplayContentFromErrorCode('unauthorized_access', extraContext);
        expect(result).toContain('Authentication failed');
    });

    it('handles rate limiting errors', () => {
        const result = getErrorDisplayContentFromErrorCode('ratelimited', extraContext);
        expect(result).toContain('rate limited');
    });
});
