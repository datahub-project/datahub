import { colors } from '@src/alchemy-components';
import { useCreateRemoteExecutorPoolMutation } from '@src/graphql/remote_executor.saas.generated';
import { Button, Form, Input, Modal, Switch, Typography } from 'antd';
import { CheckCircle } from 'phosphor-react';
import React, { useState } from 'react';
import styled from 'styled-components';

const SuccessHeaderWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    margin-bottom: 12px;
`;

type Props = {
    visible: boolean;
    onCancel: () => void;
    onSuccessfulCreate: () => void;
};

type FormProps = {
    name: string;
    description: string;
    isDefault: boolean;
};

export default function CreateRemoteExecutorPoolModal({ visible, onCancel, onSuccessfulCreate }: Props) {
    const [showSuccess, setShowSuccess] = useState(false);
    const [form] = Form.useForm<FormProps>();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(true);

    const [createPool, { loading }] = useCreateRemoteExecutorPoolMutation();

    const handleCreate = async () => {
        try {
            const { name, description, isDefault } = form.getFieldsValue();
            await createPool({
                variables: {
                    input: {
                        executorPoolId: name,
                        description,
                        isDefault,
                    },
                },
            });
            onSuccessfulCreate();
            setShowSuccess(true);
        } catch (error) {
            console.error('Failed to create pool:', error);
        }
    };

    const handleClose = () => {
        form.resetFields();
        setShowSuccess(false);
        onCancel();
    };

    if (showSuccess) {
        return (
            <Modal open={visible} cancelButtonProps={{ style: { display: 'none' } }} onOk={handleClose} title="Success">
                <div className="space-y-4 p-4">
                    <div className="flex flex-col gap-2">
                        <SuccessHeaderWrapper>
                            <CheckCircle color={colors.green[500]} size={20} />
                            <Typography.Text style={{ fontSize: 20, marginLeft: 4 }}>Pool created!</Typography.Text>
                        </SuccessHeaderWrapper>
                        <Typography.Text strong>For next steps:</Typography.Text>
                        <ul>
                            <li>
                                Review the{' '}
                                <Typography.Link href="https://datahubproject.io/docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor/">
                                    remote executor setup documentation
                                </Typography.Link>
                            </li>
                            <li>Contact your customer success representative for assistance with configuration</li>
                        </ul>
                    </div>
                </div>
            </Modal>
        );
    }

    return (
        <Modal
            open={visible}
            onCancel={handleClose}
            title="Create Remote Executor Pool"
            footer={
                <div>
                    <Button type="text" onClick={handleClose} data-testid="cancel-create-pool-button">
                        Cancel
                    </Button>
                    <Button
                        onClick={handleCreate}
                        disabled={createButtonEnabled || loading}
                        data-testid="create-pool-button"
                    >
                        Create
                    </Button>
                </div>
            }
        >
            <Form
                form={form}
                layout="vertical"
                onFieldsChange={() =>
                    setCreateButtonEnabled(form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <Form.Item
                    label={<Typography.Text>Pool Identifier</Typography.Text>}
                    name="name"
                    required={false}
                    rules={[
                        {
                            required: true,
                            message: 'Please enter a pool name',
                        },
                        {
                            validator: (_, value) =>
                                // disallow all special characters except for underline dash and dot
                                /^[a-zA-Z0-9_.-]*$/.test(value)
                                    ? Promise.resolve()
                                    : Promise.reject(new Error('Only use alphanumerics and _.-')),
                        },
                        { whitespace: true, message: 'No spaces' },
                        { min: 1, max: 50 },
                    ]}
                >
                    <div>
                        <Typography.Text type="secondary">Enter a name for your remote executor pool.</Typography.Text>
                        <div style={{ marginTop: 4 }}>
                            <Input
                                onKeyDown={(e) => {
                                    // disallow all special characters except for underline dash and dot
                                    if (
                                        !/^[a-zA-Z0-9_.-]*$/.test(e.key) &&
                                        e.key !== 'Backspace' &&
                                        e.key !== 'Delete' &&
                                        e.key !== 'ArrowLeft' &&
                                        e.key !== 'ArrowRight'
                                    ) {
                                        e.preventDefault();
                                    }
                                }}
                                placeholder="us-east-pool"
                                data-testid="create-pool-name"
                            />
                        </div>
                    </div>
                </Form.Item>

                <Form.Item
                    label={<Typography.Text>Description</Typography.Text>}
                    name="description"
                    required={false}
                    rules={[
                        {
                            required: true,
                            message: 'Help users understand what the pool is for.',
                        },
                        { whitespace: true },
                        { min: 1, max: 50 },
                    ]}
                >
                    <div>
                        <Typography.Text type="secondary">Briefly describe what this pool is used for.</Typography.Text>
                        <div style={{ marginTop: 4 }}>
                            <Input.TextArea
                                placeholder="Connect to soruces in the us-east region with this pool."
                                data-testid="create-pool-description"
                                rows={2}
                            />
                        </div>
                    </div>
                </Form.Item>

                <Form.Item
                    label={<Typography.Text className="font-bold">Default Pool</Typography.Text>}
                    name="isDefault"
                    valuePropName="checked"
                    initialValue={false}
                >
                    <div className="space-y-2">
                        <Typography.Text type="secondary">
                            Set this as the default executor pool for new ingestion sources.
                        </Typography.Text>
                        <div style={{ marginTop: 4 }}>
                            <Switch data-testid="default-pool" />
                        </div>
                    </div>
                </Form.Item>
            </Form>
        </Modal>
    );
}
