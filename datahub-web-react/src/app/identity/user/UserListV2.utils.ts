import { ColorValues } from '@components/theme/config';

import { CorpUser, CorpUserStatus, FacetFilterInput } from '@types';

export const getUserStatusColor = (userStatus: CorpUserStatus | null | undefined, _user: CorpUser): ColorValues => {
    switch (userStatus) {
        case CorpUserStatus.Active:
            return ColorValues.green;
        case CorpUserStatus.Suspended:
            return ColorValues.red;
        default:
            return ColorValues.gray;
    }
};

export const getUserStatusText = (userStatus: CorpUserStatus | undefined | null, _user: CorpUser): string => {
    if (userStatus) {
        return userStatus.charAt(0).toUpperCase() + userStatus.slice(1).toLowerCase();
    }

    // If no status, default to Inactive
    return 'Inactive';
};

type StatusFilterOption = {
    label: string;
    value: string;
    disabled?: boolean;
};

export const STATUS_FILTER_OPTIONS: StatusFilterOption[] = [
    { label: 'Status', value: 'all' },
    { label: 'Active', value: 'active' },
    { label: 'Suspended', value: 'suspended' },
];

/**
 * Builds GraphQL filter objects for user status filtering (server-side)
 * Note: Service accounts are automatically excluded by the backend searchUsers resolver
 */
export function buildFilters(statusFilter?: string): FacetFilterInput[] | undefined {
    if (!statusFilter || statusFilter === 'all') {
        return undefined;
    }

    switch (statusFilter.toLowerCase()) {
        case 'active':
            return [{ field: 'status', values: ['ACTIVE'] }];
        case 'suspended':
            return [{ field: 'status', values: ['SUSPENDED'] }];
        default:
            return undefined;
    }
}

/**
 * Extracts the current role URN from a user object
 */
export function extractUserRole(
    user: any,
    selectRoleOptions: Array<{ urn: string; name: string }>,
): string | undefined {
    const currentRoleUrn = user?.roles?.relationships?.[0]?.entity?.urn;
    if (currentRoleUrn) return currentRoleUrn;

    // Fallback: find role by name if URN missing
    const roleName = user?.roles?.relationships?.[0]?.entity?.name;
    if (roleName) {
        const matchedRole = selectRoleOptions.find((r) => r.name === roleName);
        return matchedRole?.urn;
    }

    return undefined;
}

// Role assignment constants
export const NO_ROLE_URN = '';
export const NO_ROLE_TEXT = 'No Role';
