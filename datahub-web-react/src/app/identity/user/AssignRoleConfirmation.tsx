import { Popconfirm, message } from 'antd';
import React from 'react';

import analytics, { EventType } from '@app/analytics';

import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { DataHubRole } from '@types';

type Props = {
    open: boolean;
    roleToAssign: DataHubRole | undefined;
    userUrn: string;
    username: string;
    onClose: () => void;
    onConfirm: () => void;
};

export default function AssignRoleConfirmation({ open, roleToAssign, userUrn, username, onClose, onConfirm }: Props) {
    const [batchAssignRoleMutation] = useBatchAssignRoleMutation();
    // eslint-disable-next-line
    const batchAssignRole = () => {
        batchAssignRoleMutation({
            variables: {
                input: {
                    roleUrn: roleToAssign?.urn,
                    actors: [userUrn],
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.SelectUserRoleEvent,
                        roleUrn: roleToAssign?.urn || 'undefined',
                        userUrn,
                    });
                    message.success({
                        content: roleToAssign
                            ? `Assigned role ${roleToAssign?.name} to user ${username}!`
                            : `Removed role from user ${username}!`,
                        duration: 2,
                    });
                    onConfirm();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: roleToAssign
                        ? `Failed to assign role ${roleToAssign?.name} to ${username}: \n ${e.message || ''}`
                        : `Failed to remove role from ${username}: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    const assignRoleText = roleToAssign
        ? `Would you like to assign the role ${roleToAssign?.name} to ${username}?`
        : `Would you like to remove ${username}'s existing role?`;

    return <Popconfirm title={assignRoleText} open={open} onConfirm={batchAssignRole} onCancel={onClose} />;
}
