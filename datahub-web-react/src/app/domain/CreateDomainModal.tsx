import React, { useState } from 'react';
import { message, Button, Input, Modal, Typography, Form, Collapse } from 'antd';
import { useCreateDomainMutation } from '../../graphql/domain.generated';

type Props = {
    visible: boolean;
    onClose: () => void;
    onCreate: (id: string | undefined, name: string, description: string) => void;
};

export default function CreateDomainModal({ visible, onClose, onCreate }: Props) {
    const [stagedName, setStagedName] = useState('');
    const [stagedDescription, setStagedDescription] = useState('');
    const [stagedId, setStagedId] = useState<string | undefined>(undefined);
    const [createDomainMutation] = useCreateDomainMutation();

    const onCreateDomain = () => {
        createDomainMutation({
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
                message.error({ content: `Failed to create Domain!: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.success({
                    content: `Created domain!`,
                    duration: 3,
                });
                onCreate(stagedId, stagedName, stagedDescription);
                setStagedName('');
                setStagedDescription('');
                setStagedId(undefined);
            });
        onClose();
    };

    return (
        <Modal
            title="Create new Domain"
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button onClick={onCreateDomain} disabled={stagedName === ''}>
                        Create
                    </Button>
                </>
            }
        >
            <Form layout="vertical">
                <Form.Item name="name" label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>Give your new Domain a name.</Typography.Paragraph>
                    <Input
                        placeholder="A name for your domain"
                        value={stagedName}
                        onChange={(event) => setStagedName(event.target.value)}
                    />
                </Form.Item>
                <Form.Item name="description" label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>
                        An optional description for your new domain. You can change this later.
                    </Typography.Paragraph>
                    <Input
                        placeholder="A description for your domain"
                        value={stagedDescription}
                        onChange={(event) => setStagedDescription(event.target.value)}
                    />
                </Form.Item>
                <Collapse ghost>
                    <Collapse.Panel header={<Typography.Text type="secondary">Advanced</Typography.Text>} key="1">
                        <Form.Item label={<Typography.Text strong>Domain Id</Typography.Text>}>
                            <Typography.Paragraph>
                                By default, a random UUID will be generated to uniquely identify this domain. If you'd
                                like to provide a custom id instead to more easily keep track of this domain, you may
                                provide it here.
                            </Typography.Paragraph>
                            <Input
                                placeholder="default"
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
