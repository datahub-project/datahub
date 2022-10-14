import { Button, Form, Input, Modal, Typography } from 'antd';
import React, { useState } from 'react';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';
import { SecretBuilderState } from './types';

type Props = {
    initialState?: SecretBuilderState;
    visible: boolean;
    onSubmit?: (source: SecretBuilderState, resetState: () => void) => void;
    onCancel?: () => void;
};

export const SecretBuilderModal = ({ initialState, visible, onSubmit, onCancel }: Props) => {
    const [secretBuilderState, setSecretBuilderState] = useState<SecretBuilderState>(initialState || {});
    const [createButtonEnabled, setCreateButtonEnabled] = useState(true);
    const [form] = Form.useForm();

    const setName = (name: string) => {
        setSecretBuilderState({
            ...secretBuilderState,
            name,
        });
    };

    const setValue = (value: string) => {
        setSecretBuilderState({
            ...secretBuilderState,
            value,
        });
    };

    const setDescription = (description: string) => {
        setSecretBuilderState({
            ...secretBuilderState,
            description,
        });
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createSecretButton',
    });

    function resetValues() {
        setSecretBuilderState({});
        form.resetFields();
    }

    return (
        <Modal
            width={540}
            title={<Typography.Text>Create a new Secret</Typography.Text>}
            visible={visible}
            onCancel={onCancel}
            zIndex={1051} // one higher than other modals - needed for managed ingestion forms
            footer={
                <>
                    <Button onClick={onCancel} type="text">
                        Cancel
                    </Button>
                    <Button
                        id="createSecretButton"
                        onClick={() => onSubmit?.(secretBuilderState, resetValues)}
                        disabled={createButtonEnabled}
                    >
                        Create
                    </Button>
                </>
            }
        >
            <Form
                form={form}
                initialValues={initialState}
                layout="vertical"
                onFieldsChange={() =>
                    setCreateButtonEnabled(form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <Form.Item label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>
                        Give your secret a name. This is what you&apos;ll use to reference the secret from your recipes.
                    </Typography.Paragraph>
                    <Form.Item
                        name="name"
                        rules={[
                            {
                                required: true,
                                message: 'Enter a name.',
                            },
                            { whitespace: true },
                            { min: 1, max: 50 },
                        ]}
                        hasFeedback
                    >
                        <Input
                            placeholder="A name for your secret"
                            value={secretBuilderState.name}
                            onChange={(event) => setName(event.target.value)}
                        />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Value</Typography.Text>}>
                    <Typography.Paragraph>
                        The value of your secret, which will be encrypted and stored securely within DataHub.
                    </Typography.Paragraph>
                    <Form.Item
                        name="value"
                        rules={[
                            {
                                required: true,
                                message: 'Enter a value.',
                            },
                            // { whitespace: true },
                            { min: 1 },
                        ]}
                        hasFeedback
                    >
                        <Input.TextArea
                            placeholder="The value of your secret"
                            value={secretBuilderState.value}
                            onChange={(event) => setValue(event.target.value)}
                            autoComplete="false"
                        />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>
                        An optional description to help keep track of your secret.
                    </Typography.Paragraph>
                    <Form.Item name="description" rules={[{ whitespace: true }, { min: 1, max: 500 }]} hasFeedback>
                        <Input
                            placeholder="The value of your secret"
                            value={secretBuilderState.description}
                            onChange={(event) => setDescription(event.target.value)}
                        />
                    </Form.Item>
                </Form.Item>
            </Form>
        </Modal>
    );
};
