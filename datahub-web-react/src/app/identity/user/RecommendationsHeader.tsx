import { Button, Text } from '@components';
import React from 'react';

import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { pluralize } from '@app/shared/textUtil';

import { CorpUser, DataHubRole } from '@types';

interface Props {
    totalUsers: number;
    usersWithEmails: CorpUser[];
    selectedRole?: DataHubRole;
    onRoleSelect: (role: DataHubRole | undefined) => void;
    onBulkSendInvitations: () => void;
}

/**
 * Component responsible for the recommendations section header
 * Handles role selection and bulk actions UI
 */
export default function RecommendationsHeader({
    totalUsers,
    usersWithEmails,
    selectedRole,
    onRoleSelect,
    onBulkSendInvitations,
}: Props) {
    return (
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <div>
                <Text size="lg" weight="semiBold">
                    Recommended Users
                </Text>
                <Text color="gray" size="md">
                    {totalUsers} {pluralize(totalUsers, 'user')} had platform activity in the last 30 days.
                </Text>
            </div>
            {usersWithEmails.length > 0 && (
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                    <SimpleSelectRole
                        selectedRole={selectedRole}
                        onRoleSelect={onRoleSelect}
                        size="md"
                        width="fit-content"
                    />
                    {usersWithEmails.length > 1 && (
                        <Button size="sm" onClick={onBulkSendInvitations}>
                            Send All ({usersWithEmails.length})
                        </Button>
                    )}
                </div>
            )}
        </div>
    );
}
