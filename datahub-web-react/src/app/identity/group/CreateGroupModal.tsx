import React, { useState } from 'react';
import { message, Button, Input, Modal, Typography, Form } from 'antd';
import { useCreateGroupMutation } from '../../../graphql/group.generated';

type Props = {
    visible: boolean;
    onClose: () => void;
    onCreate: (name: string, description: string) => void;
};

export default function CreateGroupModal({ visible, onClose, onCreate }: Props) {
    const [stagedName, setStagedName] = useState('');
    const [stagedDescription, setStagedDescription] = useState('');
    const [createGroupMutation] = useCreateGroupMutation();

    const onCreateGroup = () => {
        createGroupMutation({
            variables: {
                input: {
                    name: stagedName,
                    description: stagedDescription,
                },
            },
        })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create group!: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.success({
                    content: `Created group!`,
                    duration: 3,
                });
                onCreate(stagedName, stagedDescription);
                setStagedName('');
                setStagedDescription('');
            });
        onClose();
    };

    return (
        <Modal
            title="Create new group"
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button onClick={onCreateGroup} disabled={stagedName === ''}>
                        Create
                    </Button>
                </>
            }
        >
            <Form layout="vertical">
                <Form.Item name="name" label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>Give your new group a name.</Typography.Paragraph>
                    <Input
                        placeholder="A name for your group"
                        value={stagedName}
                        onChange={(event) => setStagedName(event.target.value)}
                    />
                </Form.Item>
                <Form.Item name="description" label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>An optional description for your new group.</Typography.Paragraph>
                    <Input
                        placeholder="A description for your group"
                        value={stagedDescription}
                        onChange={(event) => setStagedDescription(event.target.value)}
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
}
