import { teamsSearchClient } from '@app/shared/subscribe/drawer/teams-search-client';

describe('teamsSearchClient', () => {
    it('should return empty arrays for legacy functions', async () => {
        const usersResult = await teamsSearchClient.searchUsers('test');
        const channelsResult = await teamsSearchClient.searchChannels('test');
        const searchResult = await teamsSearchClient.search({ query: 'test' });

        expect(usersResult).toEqual([]);
        expect(channelsResult).toEqual([]);
        expect(searchResult).toEqual([]);
    });

    it('should handle search with empty query', async () => {
        const result = await teamsSearchClient.search({ query: '' });
        expect(result).toEqual([]);
    });

    it('should handle search with whitespace query', async () => {
        const result = await teamsSearchClient.search({ query: '   ' });
        expect(result).toEqual([]);
    });
});
