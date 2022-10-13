import React from 'react';
import { message, Popconfirm } from 'antd';
import { useBatchAssignRoleMutation } from '../../../graphql/mutations.generated';
import { DataHubRole } from '../../../types.generated';

type Props = {
    visible: boolean;
    roleToAssign: DataHubRole | undefined;
    userUrn: string;
    username: string;
    onClose: () => void;
    onConfirm: () => void;
};

export default function AssignRoleConfirmation({
    visible,
    roleToAssign,
    userUrn,
    username,
    onClose,
    onConfirm,
}: Props) {
    const [batchAssignRoleMutation] = useBatchAssignRoleMutation();
    // eslint-disable-next-line
    const batchAssignRole = () => {
        if (roleToAssign) {
            batchAssignRoleMutation({
                variables: {
                    input: {
                        roleUrn: roleToAssign.urn,
                        actors: [userUrn],
                    },
                },
            })
                .then(({ errors }) => {
                    if (!errors) {
                        message.success({
                            content: `Assigned role ${roleToAssign?.name} to user ${username}!`,
                            duration: 2,
                        });
                        onConfirm();
                    }
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to assign role ${roleToAssign?.name} to ${username}: \n ${e.message || ''}`,
                        duration: 3,
                    });
                });
        }
    };

    return (
        <Popconfirm
            title={`Would you like to assign the role ${roleToAssign?.name} to ${username}?`}
            visible={visible}
            onConfirm={batchAssignRole}
            onCancel={onClose}
        />
    );
}
