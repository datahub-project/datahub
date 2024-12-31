import React from 'react';
import { message, Popconfirm } from 'antd';
import { useBatchAssignRoleMutation } from '../../../graphql/mutations.generated';
import { DataHubRole } from '../../../types.generated';
import analytics, { EventType } from '../../analytics';

type Props = {
    open: boolean;
    roleToAssign: DataHubRole | undefined;
    groupName: string;
    groupUrn: string;
    onClose: () => void;
    onConfirm: () => void;
};

export default function AssignRoletoGroupConfirmation({
    open,
    roleToAssign,
    groupName,
    groupUrn,
    onClose,
    onConfirm,
}: Props) {
    const [batchAssignRoleMutation] = useBatchAssignRoleMutation();
    // eslint-disable-next-line
    const batchAssignRole = () => {
        batchAssignRoleMutation({
            variables: {
                input: {
                    roleUrn: roleToAssign?.urn,
                    actors: [groupUrn],
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.SelectGroupRoleEvent,
                        roleUrn: roleToAssign?.urn || 'undefined',
                        groupUrn,
                    });
                    message.success({
                        content: roleToAssign
                            ? `Assigned role ${roleToAssign?.name} to group ${groupName}!`
                            : `Removed role from user ${groupName}!`,
                        duration: 2,
                    });
                    onConfirm();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: roleToAssign
                        ? `Failed to assign role ${roleToAssign?.name} to group ${groupName}: \n ${e.message || ''}`
                        : `Failed to remove role from  group ${groupName}: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    const assignRoleText = roleToAssign
        ? `Would you like to assign the role ${roleToAssign?.name} to group ${groupName}?`
        : `Would you like to remove group ${groupName}'s existing role?`;

    return <Popconfirm title={assignRoleText} open={open} onConfirm={batchAssignRole} onCancel={onClose} />;
}
