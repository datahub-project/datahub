import { ColorValues } from '@components/theme/config';

import { UserListItem } from '@app/identity/user/UserAndGroupList.hooks';

import { CorpUserStatus } from '@types';

export const getUserStatusColor = (userStatus: CorpUserStatus | null | undefined): ColorValues => {
    switch (userStatus) {
        case CorpUserStatus.Active:
            return ColorValues.green;
        case CorpUserStatus.Suspended:
            return ColorValues.red;
        default:
            return ColorValues.gray;
    }
};

export const getUserStatusText = (userStatus: CorpUserStatus | undefined | null, user: UserListItem): string => {
    if (userStatus) {
        return userStatus.charAt(0).toUpperCase() + userStatus.slice(1).toLowerCase();
    }
    // If no status, check if user has logged in before
    return user.isNativeUser ? 'Invited' : 'Inactive';
};

export const filterUsersByStatus = (users: UserListItem[], statusFilter: string): UserListItem[] => {
    return users.filter((user) => {
        if (statusFilter === 'all') return true;
        const status = getUserStatusText(user.status, user).toLowerCase();
        return status === statusFilter.toLowerCase();
    });
};

export const STATUS_FILTER_OPTIONS = [
    { label: 'Status', value: 'all' },
    { label: 'Active', value: 'active' },
    { label: 'Suspended', value: 'suspended' },
    { label: 'Invited', value: 'invited' },
    { label: 'Inactive', value: 'inactive' },
];
