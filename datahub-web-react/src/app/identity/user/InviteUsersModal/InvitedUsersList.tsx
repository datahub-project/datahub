import { Avatar } from '@components';
import React from 'react';

import {
    InvitedUserItem,
    InvitedUsersLabel,
    InvitedUsersSection,
    InvitedUsersList as StyledInvitedUsersList,
    UserEmail,
    UserStatus,
} from '@app/identity/user/InviteUsersModal.components';

import { DataHubRole } from '@types';

type InvitedUser = {
    email: string;
    role?: DataHubRole;
};

type Props = {
    invitedUsers: InvitedUser[];
};

export default function InvitedUsersList({ invitedUsers }: Props) {
    if (invitedUsers.length === 0) {
        return null;
    }

    return (
        <InvitedUsersSection>
            <InvitedUsersLabel>{invitedUsers.length} Invited</InvitedUsersLabel>
            <StyledInvitedUsersList>
                {invitedUsers.map((user) => (
                    <InvitedUserItem key={user.email}>
                        <Avatar name={user.email} size="sm" />
                        <UserEmail>{user.email}</UserEmail>
                        <UserStatus>{user.role?.name || 'No Role'}</UserStatus>
                    </InvitedUserItem>
                ))}
            </StyledInvitedUsersList>
        </InvitedUsersSection>
    );
}
