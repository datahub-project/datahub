import { message } from 'antd';
import { debounce } from 'lodash';
import { useCallback, useEffect, useState } from 'react';

import { useIntegrationTeamsSearchLazyQuery } from '@graphql/integrations/teams.generated';
import { TeamsSearchType } from '@types';

export interface TeamsSearchUser {
    id: string;
    type: 'user';
    displayName: string;
    email: string;
    department?: string;
}

export interface TeamsSearchChannel {
    id: string;
    type: 'channel';
    displayName: string;
    teamName: string;
    memberCount?: number;
}

export type TeamsSearchResult = TeamsSearchUser | TeamsSearchChannel;

export interface TeamsSearchResponse {
    results: TeamsSearchResult[];
    hasMore: boolean;
}

export interface TeamsSearchOptions {
    query: string;
    type?: 'users' | 'channels' | 'all';
    limit?: number;
}

// GraphQL-based search functions following the home page pattern
export const useTeamsSearch = () => {
    const [searchTeamsQuery, { data, loading, error }] = useIntegrationTeamsSearchLazyQuery();
    const [channels, setChannels] = useState<TeamsSearchChannel[]>([]);
    const [users, setUsers] = useState<TeamsSearchUser[]>([]);

    // Handle data updates from the Apollo query
    useEffect(() => {
        if (data?.integrationTeamsSearch) {
            if (data.integrationTeamsSearch.channels) {
                const channelResults = data.integrationTeamsSearch.channels.map((channel) => ({
                    id: channel.id,
                    type: 'channel' as const,
                    displayName: channel.displayName,
                    teamName: channel.teamName,
                    memberCount: channel.memberCount || undefined,
                }));
                setChannels(channelResults);
            }

            if (data.integrationTeamsSearch.users) {
                const userResults = data.integrationTeamsSearch.users.map((user) => ({
                    id: user.id,
                    type: 'user' as const,
                    displayName: user.displayName,
                    email: user.email,
                    department: user.department || undefined,
                }));
                setUsers(userResults);
            }
        }
    }, [data, error]);

    // Debounced search function for channels - following home page pattern
    const searchChannels = debounce((query: string, limit = 10) => {
        if (query && query.trim() !== '') {
            searchTeamsQuery({
                variables: {
                    input: {
                        query: query.trim(),
                        type: TeamsSearchType.Channels,
                        limit,
                    },
                },
            });
        }
    }, 300); // 300ms debounce

    // Debounced search function for users
    const searchUsers = debounce((query: string, limit = 10) => {
        if (query && query.trim() !== '') {
            searchTeamsQuery({
                variables: {
                    input: {
                        query: query.trim(),
                        type: TeamsSearchType.Users,
                        limit,
                    },
                },
            });
        }
    }, 300);

    const search = async (options: TeamsSearchOptions): Promise<TeamsSearchResult[]> => {
        const { query, type = 'all', limit = 10 } = options;

        if (!query.trim()) {
            return [];
        }

        try {
            let searchType: TeamsSearchType;
            if (type === 'users') {
                searchType = TeamsSearchType.Users;
            } else if (type === 'channels') {
                searchType = TeamsSearchType.Channels;
            } else {
                searchType = TeamsSearchType.All;
            }

            const queryResult = (await searchTeamsQuery({
                variables: {
                    input: {
                        query: query.trim(),
                        type: searchType,
                        limit,
                    },
                },
            })) as any;

            const result = queryResult?.data;

            if (result?.integrationTeamsSearch) {
                const userResults: TeamsSearchResult[] = result.integrationTeamsSearch.users.map((user) => ({
                    id: user.id,
                    type: 'user' as const,
                    displayName: user.displayName,
                    email: user.email,
                    department: user.department || undefined,
                }));

                const channelResults: TeamsSearchResult[] = result.integrationTeamsSearch.channels.map((channel) => ({
                    id: channel.id,
                    type: 'channel' as const,
                    displayName: channel.displayName,
                    teamName: channel.teamName,
                    memberCount: channel.memberCount || undefined,
                }));

                return [...userResults, ...channelResults];
            }
            return [];
        } catch (searchError) {
            message.error('Failed to search Teams. Please try again.');
            return [];
        }
    };

    return {
        searchUsers,
        searchChannels,
        search,
        // State data that components can use
        channels,
        users,
        loading,
        error,
        // Clear functions
        clearResults: useCallback(() => {
            setChannels([]);
            setUsers([]);
        }, []),
    };
};

// Legacy client object for backward compatibility
export const teamsSearchClient = {
    searchUsers: async (_query: string, _limit?: number): Promise<TeamsSearchUser[]> => {
        return [];
    },
    searchChannels: async (_query: string, _limit?: number): Promise<TeamsSearchChannel[]> => {
        return [];
    },
    search: async (_options: TeamsSearchOptions): Promise<TeamsSearchResult[]> => {
        return [];
    },
};

// Legacy exports for backward compatibility
export const searchTeamsUsers = teamsSearchClient.searchUsers;
export const searchTeamsChannels = teamsSearchClient.searchChannels;
export const searchTeams = teamsSearchClient.search;
