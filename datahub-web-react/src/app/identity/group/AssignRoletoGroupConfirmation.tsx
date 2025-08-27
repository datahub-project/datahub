import { Popconfirm, message } from 'antd';
import React from 'react';

import analytics, { EventType } from '@app/analytics';

import { useAssignMultipleRolesMutation, useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { DataHubRole } from '@types';

type Props = {
    open: boolean;
    roleToAssign: DataHubRole | undefined;
    groupName: string;
    groupUrn: string;
    onClose: () => void;
    onConfirm: () => void;
    // Optional multi-role changes for enhanced functionality
    multiRoleChanges?: {
        rolesToAdd: DataHubRole[];
        rolesToRemove: DataHubRole[];
        currentRoles: DataHubRole[];
    };
};

export default function AssignRoletoGroupConfirmation({
    open,
    roleToAssign,
    groupName,
    groupUrn,
    onClose,
    onConfirm,
    multiRoleChanges,
}: Props) {
    const [batchAssignRoleMutation] = useBatchAssignRoleMutation();
    const [assignMultipleRolesMutation] = useAssignMultipleRolesMutation();
    // eslint-disable-next-line
    const batchAssignRole = async () => {
        try {
            // Handle multi-role changes if provided
            if (multiRoleChanges) {
                const { rolesToAdd, rolesToRemove, currentRoles } = multiRoleChanges;

                // Calculate final role state
                const finalRoleUrns = new Set(currentRoles.map((role) => role.urn));

                // Remove roles that should be removed
                rolesToRemove.forEach((role) => finalRoleUrns.delete(role.urn));

                // Add roles that should be added
                rolesToAdd.forEach((role) => finalRoleUrns.add(role.urn));

                // Use the new assignMultipleRoles mutation for atomic multi-role assignment
                await assignMultipleRolesMutation({
                    variables: {
                        input: {
                            actorUrn: groupUrn,
                            roleUrns: Array.from(finalRoleUrns),
                        },
                    },
                });

                // Analytics for multi-role changes
                rolesToAdd.forEach((role) => {
                    analytics.event({
                        type: EventType.SelectGroupRoleEvent,
                        roleUrn: role.urn,
                        groupUrn,
                    });
                });

                const totalChanges = rolesToAdd.length + rolesToRemove.length;
                message.success({
                    content: `Successfully updated ${totalChanges} role(s) for group ${groupName}!`,
                    duration: 2,
                });
            } else {
                // Handle single role assignment (backward compatibility)
                await batchAssignRoleMutation({
                    variables: {
                        input: {
                            roleUrn: roleToAssign?.urn,
                            actors: [groupUrn],
                        },
                    },
                });

                analytics.event({
                    type: EventType.SelectGroupRoleEvent,
                    roleUrn: roleToAssign?.urn || 'undefined',
                    groupUrn,
                });

                message.success({
                    content: roleToAssign
                        ? `Assigned role ${roleToAssign?.name} to group ${groupName}!`
                        : `Removed role from group ${groupName}!`,
                    duration: 2,
                });
            }

            onConfirm();
        } catch (error: any) {
            message.destroy();
            message.error({
                content: multiRoleChanges
                    ? `Failed to update roles for group ${groupName}: ${error.message || ''}`
                    : `Failed to assign role to group ${groupName}: ${error.message || ''}`,
                duration: 3,
            });
        }
    };

    const getConfirmationText = () => {
        if (multiRoleChanges) {
            const { rolesToAdd, rolesToRemove } = multiRoleChanges;
            const changes: string[] = [];

            if (rolesToAdd.length > 0) {
                changes.push(`add ${rolesToAdd.length} role(s): ${rolesToAdd.map((r) => r.name).join(', ')}`);
            }
            if (rolesToRemove.length > 0) {
                changes.push(`remove ${rolesToRemove.length} role(s): ${rolesToRemove.map((r) => r.name).join(', ')}`);
            }

            return `Would you like to ${changes.join(' and ')} for group ${groupName}?`;
        }

        return roleToAssign
            ? `Would you like to assign the role ${roleToAssign?.name} to group ${groupName}?`
            : `Would you like to remove group ${groupName}'s existing role?`;
    };

    return <Popconfirm title={getConfirmationText()} open={open} onConfirm={batchAssignRole} onCancel={onClose} />;
}
