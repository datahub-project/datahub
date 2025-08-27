import { UserListItem } from '@app/identity/user/UserAndGroupList.hooks';
import { STATUS_FILTER_OPTIONS, filterUsersByStatus, getUserStatusText } from '@app/identity/user/UserList.utils';

import { CorpUserStatus } from '@types';

// Helper function to create mock users for testing
const createMockUser = (overrides: Partial<UserListItem> = {}): UserListItem => ({
    __typename: 'CorpUser',
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

        it('should return Invited for native users without status', () => {
            const result = getUserStatusText(null, createMockUser({ isNativeUser: true, status: undefined }));
            expect(result).toBe('Invited');
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

    describe('filterUsersByStatus', () => {
        const mockUsers = [
            createMockUser({ status: CorpUserStatus.Active, isNativeUser: true }),
            createMockUser({ status: CorpUserStatus.Suspended, isNativeUser: true }),
            createMockUser({ status: undefined, isNativeUser: true }),
            createMockUser({ status: undefined, isNativeUser: false }),
        ];

        it('should return all users when status filter is "all"', () => {
            const result = filterUsersByStatus(mockUsers, 'all');
            expect(result).toHaveLength(4);
        });

        it('should filter active users correctly', () => {
            const result = filterUsersByStatus(mockUsers, 'active');
            expect(result).toHaveLength(1);
            expect(result[0].status).toBe(CorpUserStatus.Active);
        });

        it('should filter suspended users correctly', () => {
            const result = filterUsersByStatus(mockUsers, 'suspended');
            expect(result).toHaveLength(1);
            expect(result[0].status).toBe(CorpUserStatus.Suspended);
        });

        it('should filter invited users correctly', () => {
            const result = filterUsersByStatus(mockUsers, 'invited');
            expect(result).toHaveLength(1);
            expect(result[0].isNativeUser).toBe(true);
            expect(result[0].status).toBeUndefined();
        });

        it('should filter inactive users correctly', () => {
            const result = filterUsersByStatus(mockUsers, 'inactive');
            expect(result).toHaveLength(1);
            expect(result[0].isNativeUser).toBe(false);
            expect(result[0].status).toBeUndefined();
        });

        it('should be case insensitive', () => {
            const result = filterUsersByStatus(mockUsers, 'ACTIVE');
            expect(result).toHaveLength(1);
            expect(result[0].status).toBe(CorpUserStatus.Active);
        });
    });

    describe('STATUS_FILTER_OPTIONS', () => {
        it('should contain all expected filter options', () => {
            expect(STATUS_FILTER_OPTIONS).toHaveLength(5);
            expect(STATUS_FILTER_OPTIONS).toEqual([
                { label: 'Status', value: 'all' },
                { label: 'Active', value: 'active' },
                { label: 'Suspended', value: 'suspended' },
                { label: 'Invited', value: 'invited' },
                { label: 'Inactive', value: 'inactive' },
            ]);
        });
    });
});
