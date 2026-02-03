import { buildFilters, extractUserRole, getUserStatusText } from '@app/identity/user/UserListV2.utils';

import { CorpUser, CorpUserStatus, EntityType } from '@types';

// Helper function to create mock users for testing
const createMockUser = (overrides: Partial<CorpUser> = {}): CorpUser => ({
    __typename: 'CorpUser',
    type: EntityType.CorpUser,
    urn: 'urn:li:corpuser:test',
    username: 'test',
    isNativeUser: false,
    status: CorpUserStatus.Active,
    ...overrides,
});

describe('UserList.utils', () => {
    describe('getUserStatusText', () => {
        it('should return Active for active users', () => {
            const result = getUserStatusText(CorpUserStatus.Active, createMockUser());
            expect(result).toBe('Active');
        });

        it('should return Suspended for suspended users', () => {
            const result = getUserStatusText(CorpUserStatus.Suspended, createMockUser());
            expect(result).toBe('Suspended');
        });

        it('should return Inactive for native users without status', () => {
            const result = getUserStatusText(null, createMockUser({ isNativeUser: true, status: undefined }));
            expect(result).toBe('Inactive');
        });

        it('should return Inactive for non-native users without status', () => {
            const result = getUserStatusText(null, createMockUser({ isNativeUser: false, status: undefined }));
            expect(result).toBe('Inactive');
        });

        it('should return Inactive for undefined status with non-native user', () => {
            const result = getUserStatusText(undefined, createMockUser({ isNativeUser: false, status: undefined }));
            expect(result).toBe('Inactive');
        });
    });

    describe('buildFilters', () => {
        it('should return undefined for "all" or undefined status', () => {
            expect(buildFilters('all')).toBeUndefined();
            expect(buildFilters()).toBeUndefined();
        });

        it('should return ACTIVE filter', () => {
            expect(buildFilters('active')).toEqual([{ field: 'status', values: ['ACTIVE'] }]);
        });

        it('should return SUSPENDED filter', () => {
            expect(buildFilters('suspended')).toEqual([{ field: 'status', values: ['SUSPENDED'] }]);
        });

        it('should be case insensitive', () => {
            expect(buildFilters('ACTIVE')).toEqual([{ field: 'status', values: ['ACTIVE'] }]);
            expect(buildFilters('SUSPENDED')).toEqual([{ field: 'status', values: ['SUSPENDED'] }]);
        });

        it('should return undefined for unknown status', () => {
            expect(buildFilters('unknown')).toBeUndefined();
            expect(buildFilters('inactive')).toBeUndefined();
        });
    });

    describe('extractUserRole', () => {
        const mockRoles = [
            { urn: 'urn:li:dataHubRole:Admin', name: 'Admin' },
            { urn: 'urn:li:dataHubRole:Editor', name: 'Editor' },
        ];

        it('should extract role URN from user with role', () => {
            const user = {
                roles: {
                    relationships: [{ entity: { urn: 'urn:li:dataHubRole:Admin', name: 'Admin' } }],
                },
            };
            expect(extractUserRole(user, mockRoles)).toBe('urn:li:dataHubRole:Admin');
        });

        it('should return undefined for user with no roles', () => {
            const userWithNull = { roles: null };
            const userWithEmptyRelationships = { roles: { relationships: [] } };

            expect(extractUserRole(userWithNull, mockRoles)).toBeUndefined();
            expect(extractUserRole(userWithEmptyRelationships, mockRoles)).toBeUndefined();
        });

        it('should fallback to name matching if URN missing', () => {
            const user = {
                roles: {
                    relationships: [{ entity: { name: 'Editor' } }],
                },
            };
            expect(extractUserRole(user, mockRoles)).toBe('urn:li:dataHubRole:Editor');
        });

        it('should return undefined if name match fails', () => {
            const user = {
                roles: {
                    relationships: [{ entity: { name: 'Unknown' } }],
                },
            };
            expect(extractUserRole(user, mockRoles)).toBeUndefined();
        });
    });
});
