import React, { useState } from 'react';
import { message, Button, Input, Modal, Typography, Form, Collapse } from 'antd';
import { useCreateGroupMutation } from '../../../graphql/group.generated';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';

type Props = {
    visible: boolean;
    onClose: () => void;
    onCreate: (name: string, description: string) => void;
};

export default function CreateGroupModal({ visible, onClose, onCreate }: Props) {
    const [stagedName, setStagedName] = useState('');
    const [stagedDescription, setStagedDescription] = useState('');
    const [stagedId, setStagedId] = useState<string | undefined>(undefined);
    const [createGroupMutation] = useCreateGroupMutation();

    const onCreateGroup = () => {
        createGroupMutation({
            variables: {
                input: {
                    id: stagedId,
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

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createGroupButton',
    });

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
                    <Button id="createGroupButton" onClick={onCreateGroup} disabled={stagedName === ''}>
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
                <Collapse ghost>
                    <Collapse.Panel header={<Typography.Text type="secondary">Advanced</Typography.Text>} key="1">
                        <Form.Item label={<Typography.Text strong>Group Id</Typography.Text>}>
                            <Typography.Paragraph>
                                By default, a random UUID will be generated to uniquely identify this group. If
                                you&apos;d like to provide a custom id instead to more easily keep track of this group,
                                you may provide it here. Be careful, you cannot easily change the group id after
                                creation.
                            </Typography.Paragraph>
                            <Input
                                placeholder="product_engineering"
                                value={stagedId || ''}
                                onChange={(event) => setStagedId(event.target.value)}
                            />
                        </Form.Item>
                    </Collapse.Panel>
                </Collapse>
            </Form>
        </Modal>
    );
}
