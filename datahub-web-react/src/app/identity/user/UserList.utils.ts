import { ColorValues } from '@components/theme/config';

import { UserListItem } from '@app/identity/user/UserAndGroupList.hooks';

import { CorpUserStatus } from '@types';

export const getUserStatusColor = (userStatus: CorpUserStatus | null | undefined, user: UserListItem): ColorValues => {
    switch (userStatus) {
        case CorpUserStatus.Active:
            return ColorValues.green;
        case CorpUserStatus.Suspended:
            return ColorValues.red;
        default:
            // Check invitation status for appropriate color
            if (user.invitationStatus?.status === 'SENT') {
                return ColorValues.blue; // Blue for invited users
            }
            return ColorValues.gray;
    }
};

export const getUserStatusText = (userStatus: CorpUserStatus | undefined | null, user: UserListItem): string => {
    if (userStatus) {
        return userStatus.charAt(0).toUpperCase() + userStatus.slice(1).toLowerCase();
    }

    // Check if user has an invitation status
    if (user.invitationStatus) {
        if (user.invitationStatus.status === 'SENT') {
            return 'Invited';
        }
        if (user.invitationStatus.status === 'ACCEPTED') {
            return 'Active'; // User was invited and accepted
        }
        if (user.invitationStatus.status === 'REVOKED') {
            return 'Inactive';
        }
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

type StatusFilterOption = {
    label: string;
    value: string;
    disabled?: boolean;
};

export const STATUS_FILTER_OPTIONS: StatusFilterOption[] = [
    { label: 'Status', value: 'all' },
    { label: 'Active', value: 'active' },
    { label: 'Invited', value: 'invited' },
    // { label: 'Inactive', value: 'inactive'}, // disabled for now: filtering on status=null doesn't seem to work
    // { label: 'Suspended', value: 'suspended' },
];
