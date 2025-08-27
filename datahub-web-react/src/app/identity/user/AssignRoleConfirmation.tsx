import { Popconfirm, message } from 'antd';
import React from 'react';

import analytics, { EventType } from '@app/analytics';

import { useBatchAssignRoleMutation, useAssignMultipleRolesMutation } from '@graphql/mutations.generated';
import { DataHubRole } from '@types';

type Props = {
    open: boolean;
    roleToAssign: DataHubRole | undefined;
    userUrn: string;
    username: string;
    onClose: () => void;
    onConfirm: () => void;
    // Optional multi-role changes for enhanced functionality
    multiRoleChanges?: {
        rolesToAdd: DataHubRole[];
        rolesToRemove: DataHubRole[];
        currentRoles: DataHubRole[];
    };
};

export default function AssignRoleConfirmation({
    open,
    roleToAssign,
    userUrn,
    username,
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
                const finalRoleUrns = new Set(currentRoles.map(role => role.urn));
                
                // Remove roles that should be removed
                rolesToRemove.forEach(role => finalRoleUrns.delete(role.urn));
                
                // Add roles that should be added
                rolesToAdd.forEach(role => finalRoleUrns.add(role.urn));
                
                // Use the new assignMultipleRoles mutation for atomic multi-role assignment
                await assignMultipleRolesMutation({
                    variables: {
                        input: {
                            actorUrn: userUrn,
                            roleUrns: Array.from(finalRoleUrns),
                        },
                    },
                });

                // Analytics for multi-role changes
                rolesToAdd.forEach((role) => {
                    analytics.event({
                        type: EventType.SelectUserRoleEvent,
                        roleUrn: role.urn,
                        userUrn,
                    });
                });

                const totalChanges = rolesToAdd.length + rolesToRemove.length;
                message.success({
                    content: `Successfully updated ${totalChanges} role(s) for user ${username}!`,
                    duration: 2,
                });
            } else {
                // Handle single role assignment (backward compatibility)
                await batchAssignRoleMutation({
                    variables: {
                        input: {
                            roleUrn: roleToAssign?.urn,
                            actors: [userUrn],
                        },
                    },
                });

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
            }

            onConfirm();
        } catch (error: any) {
            message.destroy();
            message.error({
                content: multiRoleChanges
                    ? `Failed to update roles for ${username}: ${error.message || ''}`
                    : `Failed to assign role to ${username}: ${error.message || ''}`,
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

            return `Would you like to ${changes.join(' and ')} for ${username}?`;
        }

        return roleToAssign
            ? `Would you like to assign the role ${roleToAssign?.name} to ${username}?`
            : `Would you like to remove ${username}'s existing role?`;
    };

    return <Popconfirm title={getConfirmationText()} open={open} onConfirm={batchAssignRole} onCancel={onClose} />;
}
