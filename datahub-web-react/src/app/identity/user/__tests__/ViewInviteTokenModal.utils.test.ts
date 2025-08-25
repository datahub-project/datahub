import {
    createUserInviteLink,
    formatNumber,
    generateUserRecommendations,
} from '@app/identity/user/ViewInviteTokenModal.utils';

import { CorpUser } from '@types';

// Mock CorpUser data for testing
const createMockUser = (
    urn: string,
    username: string,
    queryCount?: number,
    platformCount?: number,
    hasInvitation?: boolean,
): CorpUser =>
    ({
        urn,
        username,
        properties: {
            displayName: `User ${username}`,
            title: 'Software Engineer',
        },
        usageFeatures: queryCount
            ? {
                  userUsageTotalPast30Days: queryCount,
                  userPlatformUsageTotalsPast30Days: Array.from({ length: platformCount || 0 }, (_, i) => ({
                      key: `platform${i}`,
                      value: 100,
                  })),
                  userPlatformUsagePercentilePast30Days: [],
                  userTopDatasetsByUsage: [],
              }
            : undefined,
        invitationStatus: hasInvitation
            ? {
                  status: 'SENT' as any,
                  created: { time: 1234567890, actor: 'urn:li:corpuser:admin' },
                  lastUpdated: { time: 1234567890, actor: 'urn:li:corpuser:admin' },
                  invitationToken: 'token123',
              }
            : undefined,
    }) as CorpUser;

describe('ViewInviteTokenModal.utils', () => {
    describe('formatNumber', () => {
        it('should format numbers less than 1000 as-is', () => {
            expect(formatNumber(0)).toBe('0');
            expect(formatNumber(42)).toBe('42');
            expect(formatNumber(999)).toBe('999');
        });

        it('should format numbers 1000 and above with k suffix', () => {
            expect(formatNumber(1000)).toBe('1.0k');
            expect(formatNumber(1500)).toBe('1.5k');
            expect(formatNumber(2400)).toBe('2.4k');
            expect(formatNumber(10000)).toBe('10.0k');
        });

        it('should handle decimal rounding correctly', () => {
            expect(formatNumber(1234)).toBe('1.2k');
            expect(formatNumber(1567)).toBe('1.6k');
            expect(formatNumber(1999)).toBe('2.0k');
        });
    });

    describe('generateUserRecommendations', () => {
        it('should filter users with no usage data', () => {
            const users = [
                createMockUser('urn:li:corpuser:user1', 'user1', 100),
                createMockUser('urn:li:corpuser:user2', 'user2'), // No usage data
                createMockUser('urn:li:corpuser:user3', 'user3', 200),
            ];

            const recommendations = generateUserRecommendations(users);

            expect(recommendations).toHaveLength(2);
            expect(recommendations[0].username).toBe('user3'); // Sorted by query count desc
            expect(recommendations[1].username).toBe('user1');
        });

        it('should filter users with zero query count', () => {
            const users = [
                createMockUser('urn:li:corpuser:user1', 'user1', 100),
                createMockUser('urn:li:corpuser:user2', 'user2', 0), // Zero queries
                createMockUser('urn:li:corpuser:user3', 'user3', 200),
            ];

            const recommendations = generateUserRecommendations(users);

            expect(recommendations).toHaveLength(2);
            expect(recommendations.map((u) => u.username)).toEqual(['user3', 'user1']);
        });

        it('should filter users who already have invitations', () => {
            const users = [
                createMockUser('urn:li:corpuser:user1', 'user1', 100),
                createMockUser('urn:li:corpuser:user2', 'user2', 300, 2, true), // Has invitation
                createMockUser('urn:li:corpuser:user3', 'user3', 200),
            ];

            const recommendations = generateUserRecommendations(users);

            expect(recommendations).toHaveLength(2);
            expect(recommendations.map((u) => u.username)).toEqual(['user3', 'user1']);
        });

        it('should sort users by query count in descending order', () => {
            const users = [
                createMockUser('urn:li:corpuser:user1', 'user1', 100),
                createMockUser('urn:li:corpuser:user2', 'user2', 500),
                createMockUser('urn:li:corpuser:user3', 'user3', 200),
                createMockUser('urn:li:corpuser:user4', 'user4', 300),
            ];

            const recommendations = generateUserRecommendations(users);

            expect(recommendations).toHaveLength(4);
            expect(recommendations.map((u) => u.username)).toEqual(['user2', 'user4', 'user3', 'user1']);
        });

        it('should respect maxRecommendations limit', () => {
            const users = Array.from({ length: 20 }, (_, i) =>
                createMockUser(`urn:li:corpuser:user${i}`, `user${i}`, 100 + i),
            );

            const recommendations = generateUserRecommendations(users, 5);

            expect(recommendations).toHaveLength(5);
            // Should get the top 5 by query count
            expect(recommendations[0].username).toBe('user19'); // Highest query count
            expect(recommendations[4].username).toBe('user15');
        });

        it('should handle empty input gracefully', () => {
            const recommendations = generateUserRecommendations([]);
            expect(recommendations).toHaveLength(0);
        });

        it('should handle users with undefined usage features', () => {
            const users = [
                createMockUser('urn:li:corpuser:user1', 'user1', 100),
                {
                    urn: 'urn:li:corpuser:user2',
                    username: 'user2',
                    usageFeatures: undefined,
                } as CorpUser,
            ];

            const recommendations = generateUserRecommendations(users);

            expect(recommendations).toHaveLength(1);
            expect(recommendations[0].username).toBe('user1');
        });
    });

    describe('createUserInviteLink', () => {
        it('should append user hint parameter to base invite link', () => {
            const baseLink = 'https://example.com/signup?invite_token=abc123';
            const username = 'john.doe';

            const result = createUserInviteLink(baseLink, username);

            expect(result).toBe('https://example.com/signup?invite_token=abc123&user_hint=john.doe');
        });

        it('should encode special characters in username', () => {
            const baseLink = 'https://example.com/signup?invite_token=abc123';
            const username = 'john doe@example.com';

            const result = createUserInviteLink(baseLink, username);

            expect(result).toBe('https://example.com/signup?invite_token=abc123&user_hint=john%20doe%40example.com');
        });

        it('should handle links that already have query parameters', () => {
            const baseLink = 'https://example.com/signup?invite_token=abc123&role=admin';
            const username = 'jane.smith';

            const result = createUserInviteLink(baseLink, username);

            expect(result).toBe('https://example.com/signup?invite_token=abc123&role=admin&user_hint=jane.smith');
        });

        it('should handle empty username', () => {
            const baseLink = 'https://example.com/signup?invite_token=abc123';
            const username = '';

            const result = createUserInviteLink(baseLink, username);

            expect(result).toBe('https://example.com/signup?invite_token=abc123&user_hint=');
        });
    });
});
