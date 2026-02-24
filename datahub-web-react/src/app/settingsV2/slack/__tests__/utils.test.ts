import { MockedProvider } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import {
    SLACK_CONNECTION_ID,
    SLACK_CONNECTION_URN,
    SLACK_INTEGRATION_PLATFORM,
    decodeSlackConnection,
    getSlackConnection,
    isSlackIntegrationConfigured,
    isSlackIntegrationConfiguredFromSettings,
    useIsSlackIntegrationConfigured,
} from '@app/settingsV2/slack/utils';

// Mock the GraphQL query
import { useConnectionQuery } from '@graphql/connection.generated';

vi.mock('@graphql/connection.generated');

const mockUseConnectionQuery = vi.mocked(useConnectionQuery);

describe('Slack Integration Utils', () => {
    describe('Constants', () => {
        it('exports correct Slack connection ID', () => {
            expect(SLACK_CONNECTION_ID).toBe('__system_slack-0');
        });

        it('exports correct Slack connection URN', () => {
            expect(SLACK_CONNECTION_URN).toBe('urn:li:dataHubConnection:__system_slack-0');
        });

        it('exports correct Slack integration platform identifier', () => {
            expect(SLACK_INTEGRATION_PLATFORM).toBe('slack');
        });
    });

    describe('decodeSlackConnection', () => {
        it('decodes valid Slack connection JSON with bot_token', () => {
            const jsonString = JSON.stringify({
                bot_token: 'xoxb-test-token',
                team_id: 'T12345678',
                app_details: {
                    client_id: 'test-client-id',
                },
            });

            const result = decodeSlackConnection(jsonString);
            expect(result.bot_token).toBe('xoxb-test-token');
            expect(result.team_id).toBe('T12345678');
            expect(result.app_details).toEqual({ client_id: 'test-client-id' });
        });

        it('returns empty values for invalid JSON', () => {
            const result = decodeSlackConnection('invalid-json');
            expect(result.bot_token).toBe('');
            expect(result.team_id).toBe('');
            expect(result.app_details).toBeNull();
        });

        it('returns empty values when fields missing', () => {
            const jsonString = JSON.stringify({ other_data: 'value' });
            const result = decodeSlackConnection(jsonString);
            expect(result.bot_token).toBe('');
            expect(result.team_id).toBe('');
            expect(result.app_details).toBeNull();
        });

        it('handles partial data correctly', () => {
            const jsonString = JSON.stringify({
                bot_token: 'xoxb-test-token',
            });
            const result = decodeSlackConnection(jsonString);
            expect(result.bot_token).toBe('xoxb-test-token');
            expect(result.team_id).toBe('');
            expect(result.app_details).toBeNull();
        });
    });

    describe('getSlackConnection', () => {
        it('extracts Slack connection data when bot_token present', () => {
            const connectionData = {
                connection: {
                    details: {
                        json: {
                            blob: JSON.stringify({
                                bot_token: 'xoxb-test-token',
                                team_id: 'T12345678',
                            }),
                        },
                    },
                },
            };

            const result = getSlackConnection(connectionData);
            expect(result).toEqual({
                bot_token: 'xoxb-test-token',
                team_id: 'T12345678',
                app_details: null,
            });
        });

        it('extracts Slack connection data when app_details present', () => {
            const connectionData = {
                connection: {
                    details: {
                        json: {
                            blob: JSON.stringify({
                                bot_token: '',
                                team_id: 'T12345678',
                                app_details: { client_id: 'test-client' },
                            }),
                        },
                    },
                },
            };

            const result = getSlackConnection(connectionData);
            expect(result?.app_details).toEqual({ client_id: 'test-client' });
        });

        it('returns null when connection data missing', () => {
            const result = getSlackConnection(null);
            expect(result).toBeNull();
        });

        it('returns null when connection missing', () => {
            const connectionData = { connection: null };
            const result = getSlackConnection(connectionData);
            expect(result).toBeNull();
        });

        it('returns null when bot_token and app_details both empty', () => {
            const connectionData = {
                connection: {
                    details: {
                        json: {
                            blob: JSON.stringify({
                                bot_token: '',
                                team_id: 'T12345678',
                                app_details: null,
                            }),
                        },
                    },
                },
            };

            const result = getSlackConnection(connectionData);
            expect(result).toBeNull();
        });

        it('returns null when JSON parsing fails', () => {
            const connectionData = {
                connection: {
                    details: {
                        json: {
                            blob: 'invalid-json',
                        },
                    },
                },
            };

            const result = getSlackConnection(connectionData);
            expect(result).toBeNull();
        });
    });

    describe('isSlackIntegrationConfigured', () => {
        it('returns true when connection has valid bot_token', () => {
            const connectionData = {
                connection: {
                    details: {
                        json: {
                            blob: JSON.stringify({
                                bot_token: 'xoxb-test-token',
                            }),
                        },
                    },
                },
            };

            const result = isSlackIntegrationConfigured(connectionData);
            expect(result).toBe(true);
        });

        it('returns true when connection has valid app_details', () => {
            const connectionData = {
                connection: {
                    details: {
                        json: {
                            blob: JSON.stringify({
                                app_details: { client_id: 'test-client' },
                            }),
                        },
                    },
                },
            };

            const result = isSlackIntegrationConfigured(connectionData);
            expect(result).toBe(true);
        });

        it('returns false when connection data missing', () => {
            const result = isSlackIntegrationConfigured(null);
            expect(result).toBe(false);
        });

        it('returns false when bot_token and app_details empty', () => {
            const connectionData = {
                connection: {
                    details: {
                        json: {
                            blob: JSON.stringify({
                                bot_token: '',
                                app_details: null,
                            }),
                        },
                    },
                },
            };

            const result = isSlackIntegrationConfigured(connectionData);
            expect(result).toBe(false);
        });
    });

    describe('isSlackIntegrationConfiguredFromSettings', () => {
        it('returns true when defaultChannelName is set', () => {
            const globalSettings = {
                globalSettings: {
                    integrationSettings: {
                        slackSettings: {
                            defaultChannelName: '#general',
                        },
                    },
                },
            };

            const result = isSlackIntegrationConfiguredFromSettings(globalSettings);
            expect(result).toBe(true);
        });

        it('returns false when defaultChannelName is empty', () => {
            const globalSettings = {
                globalSettings: {
                    integrationSettings: {
                        slackSettings: {
                            defaultChannelName: '',
                        },
                    },
                },
            };

            const result = isSlackIntegrationConfiguredFromSettings(globalSettings);
            expect(result).toBe(false);
        });

        it('returns false when slackSettings missing', () => {
            const globalSettings = {
                globalSettings: {
                    integrationSettings: {},
                },
            };

            const result = isSlackIntegrationConfiguredFromSettings(globalSettings);
            expect(result).toBe(false);
        });

        it('returns false when globalSettings is null', () => {
            const result = isSlackIntegrationConfiguredFromSettings(null);
            expect(result).toBe(false);
        });
    });

    describe('useIsSlackIntegrationConfigured', () => {
        const wrapper = ({ children }: { children: React.ReactNode }) =>
            React.createElement(MockedProvider, {}, children);

        beforeEach(() => {
            vi.clearAllMocks();
        });

        it('returns false when query is loading', () => {
            mockUseConnectionQuery.mockReturnValue({
                data: undefined,
                loading: true,
                error: undefined,
            } as any);

            const { result } = renderHook(() => useIsSlackIntegrationConfigured(), { wrapper });
            expect(result.current).toBe(false);
        });

        it('returns false when query fails', () => {
            mockUseConnectionQuery.mockReturnValue({
                data: undefined,
                loading: false,
                error: new Error('Query failed'),
            } as any);

            const { result } = renderHook(() => useIsSlackIntegrationConfigured(), { wrapper });
            expect(result.current).toBe(false);
        });

        it('returns true when connection has a name', () => {
            mockUseConnectionQuery.mockReturnValue({
                data: {
                    connection: {
                        details: {
                            name: 'Slack Connection',
                        },
                    },
                },
                loading: false,
                error: undefined,
            } as any);

            const { result } = renderHook(() => useIsSlackIntegrationConfigured(), { wrapper });
            expect(result.current).toBe(true);
        });

        it('returns false when connection has no name', () => {
            mockUseConnectionQuery.mockReturnValue({
                data: {
                    connection: {
                        details: {
                            name: '',
                        },
                    },
                },
                loading: false,
                error: undefined,
            } as any);

            const { result } = renderHook(() => useIsSlackIntegrationConfigured(), { wrapper });
            expect(result.current).toBe(false);
        });

        it('uses cache-first fetch policy to avoid repeated expensive queries', () => {
            mockUseConnectionQuery.mockReturnValue({
                data: undefined,
                loading: true,
                error: undefined,
            } as any);

            renderHook(() => useIsSlackIntegrationConfigured(), { wrapper });

            expect(mockUseConnectionQuery).toHaveBeenCalledWith({
                variables: { urn: SLACK_CONNECTION_URN },
                errorPolicy: 'ignore',
                fetchPolicy: 'cache-first',
            });
        });
    });
});
