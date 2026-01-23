import { ColorValues } from '@components/theme/config';

import { CorpUser, CorpUserStatus } from '@types';

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

export const filterUsersByStatus = (users: CorpUser[], statusFilter: string): CorpUser[] => {
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
    { label: 'Suspended', value: 'suspended' },
];
