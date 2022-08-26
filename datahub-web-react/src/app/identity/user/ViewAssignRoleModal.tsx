import React from 'react';
import { Button, message, Modal, Typography } from 'antd';
import { useBatchAssignRoleToActorsMutation } from '../../../graphql/mutations.generated';
import { Role } from '../../../types.generated';

type Props = {
    visible: boolean;
    roleToAssign: Role | undefined;
    userUrn: string;
    username: string;
    onClose: () => void;
    onConfirm: () => void;
};

export default function ViewAssignRoleModal({ visible, roleToAssign, userUrn, username, onClose, onConfirm }: Props) {
    const [batchAssignRoleToActorsMutation] = useBatchAssignRoleToActorsMutation();
    // eslint-disable-next-line
    const batchAssignRoleToActors = () => {
        if (roleToAssign) {
            batchAssignRoleToActorsMutation({
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
        <Modal
            width={700}
            title="Assign role"
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        No
                    </Button>
                    <Button
                        onClick={() => {
                            batchAssignRoleToActors();
                        }}
                    >
                        Yes
                    </Button>
                </>
            }
        >
            {' '}
            <Typography.Text>
                <b>{`Would you like to assign the role ${roleToAssign?.name} to ${username}?`}</b>
            </Typography.Text>
        </Modal>
    );
}
