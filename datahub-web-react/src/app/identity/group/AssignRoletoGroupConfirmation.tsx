/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Popconfirm, message } from 'antd';
import React from 'react';

import analytics, { EventType } from '@app/analytics';

import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { DataHubRole } from '@types';

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
