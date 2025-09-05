import { MockedProvider } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import {
    TEAMS_CONNECTION_URN,
    TEAMS_INTEGRATION_PLATFORM,
    decodeTeamsConnection,
    getTeamsConnection,
    isMSFTTeamsIntegrationConfigured,
    useIsMSFTTeamsIntegrationConfigured,
} from '@app/settingsV2/teams/utils';

// Mock the GraphQL query
import { useConnectionQuery } from '@graphql/connection.generated';

vi.mock('@graphql/connection.generated');

const mockUseConnectionQuery = vi.mocked(useConnectionQuery);

describe('Teams Integration Utils', () => {
    describe('Constants', () => {
        it('exports correct Teams connection URN', () => {
            expect(TEAMS_CONNECTION_URN).toBe('urn:li:dataHubConnection:__system_teams-0');
        });

        it('exports correct Teams integration platform identifier', () => {
            expect(TEAMS_INTEGRATION_PLATFORM).toBe('teams');
        });
    });

    describe('decodeTeamsConnection', () => {
        it('decodes valid Teams connection JSON', () => {
            const jsonString = JSON.stringify({
                app_details: {
                    tenant_id: 'test-tenant-123',
                    app_id: 'test-app-id',
                },
            });

            const result = decodeTeamsConnection(jsonString);
            expect(result.tenant_id).toBe('test-tenant-123');
        });

        it('returns empty tenant_id for invalid JSON', () => {
            const result = decodeTeamsConnection('invalid-json');
            expect(result.tenant_id).toBe('');
        });

        it('returns empty tenant_id when app_details missing', () => {
            const jsonString = JSON.stringify({ other_data: 'value' });
            const result = decodeTeamsConnection(jsonString);
            expect(result.tenant_id).toBe('');
        });

        it('returns empty tenant_id when tenant_id missing', () => {
            const jsonString = JSON.stringify({
                app_details: {
                    app_id: 'test-app-id',
                },
            });
            const result = decodeTeamsConnection(jsonString);
            expect(result.tenant_id).toBe('');
        });
    });

    describe('getTeamsConnection', () => {
        it('extracts Teams connection data when valid', () => {
            const connectionData = {
                connection: {
                    details: {
                        json: {
                            blob: JSON.stringify({
                                app_details: {
                                    tenant_id: 'test-tenant-123',
                                    app_id: 'test-app-id',
                                },
                            }),
                        },
                    },
                },
            };

            const result = getTeamsConnection(connectionData);
            expect(result).toEqual({ tenant_id: 'test-tenant-123' });
        });

        it('returns null when connection data missing', () => {
            const result = getTeamsConnection(null);
            expect(result).toBeNull();
        });

        it('returns null when connection missing', () => {
            const connectionData = { connection: null };
            const result = getTeamsConnection(connectionData);
            expect(result).toBeNull();
        });

        it('returns null when tenant_id empty', () => {
            const connectionData = {
                connection: {
                    details: {
                        json: {
                            blob: JSON.stringify({
                                app_details: {
                                    tenant_id: '',
                                    app_id: 'test-app-id',
                                },
                            }),
                        },
                    },
                },
            };

            const result = getTeamsConnection(connectionData);
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

            const result = getTeamsConnection(connectionData);
            expect(result).toBeNull();
        });
    });

    describe('isMSFTTeamsIntegrationConfigured', () => {
        it('returns true when connection has valid tenant_id', () => {
            const connectionData = {
                connection: {
                    details: {
                        json: {
                            blob: JSON.stringify({
                                app_details: {
                                    tenant_id: 'test-tenant-123',
                                },
                            }),
                        },
                    },
                },
            };

            const result = isMSFTTeamsIntegrationConfigured(connectionData);
            expect(result).toBe(true);
        });

        it('returns false when connection data missing', () => {
            const result = isMSFTTeamsIntegrationConfigured(null);
            expect(result).toBe(false);
        });

        it('returns false when tenant_id empty', () => {
            const connectionData = {
                connection: {
                    details: {
                        json: {
                            blob: JSON.stringify({
                                app_details: {
                                    tenant_id: '',
                                },
                            }),
                        },
                    },
                },
            };

            const result = isMSFTTeamsIntegrationConfigured(connectionData);
            expect(result).toBe(false);
        });
    });

    describe('useIsMSFTTeamsIntegrationConfigured', () => {
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

            const { result } = renderHook(() => useIsMSFTTeamsIntegrationConfigured(), { wrapper });
            expect(result.current).toBe(false);
        });

        it('returns true when query fails (fallback)', () => {
            mockUseConnectionQuery.mockReturnValue({
                data: undefined,
                loading: false,
                error: new Error('Query failed'),
            } as any);

            const { result } = renderHook(() => useIsMSFTTeamsIntegrationConfigured(), { wrapper });
            expect(result.current).toBe(true);
        });

        it('returns true when connection has valid tenant_id', () => {
            mockUseConnectionQuery.mockReturnValue({
                data: {
                    connection: {
                        details: {
                            json: {
                                blob: JSON.stringify({
                                    app_details: {
                                        tenant_id: 'test-tenant-123',
                                    },
                                }),
                            },
                        },
                    },
                },
                loading: false,
                error: undefined,
            } as any);

            const { result } = renderHook(() => useIsMSFTTeamsIntegrationConfigured(), { wrapper });
            expect(result.current).toBe(true);
        });

        it('returns false when connection has no tenant_id', () => {
            mockUseConnectionQuery.mockReturnValue({
                data: {
                    connection: {
                        details: {
                            json: {
                                blob: JSON.stringify({
                                    app_details: {
                                        tenant_id: '',
                                    },
                                }),
                            },
                        },
                    },
                },
                loading: false,
                error: undefined,
            } as any);

            const { result } = renderHook(() => useIsMSFTTeamsIntegrationConfigured(), { wrapper });
            expect(result.current).toBe(false);
        });

        it('uses cache-first fetch policy to avoid repeated expensive queries', () => {
            mockUseConnectionQuery.mockReturnValue({
                data: undefined,
                loading: true,
                error: undefined,
            } as any);

            renderHook(() => useIsMSFTTeamsIntegrationConfigured(), { wrapper });

            expect(mockUseConnectionQuery).toHaveBeenCalledWith({
                variables: { urn: TEAMS_CONNECTION_URN },
                errorPolicy: 'ignore',
                fetchPolicy: 'cache-first',
            });
        });
    });
});
