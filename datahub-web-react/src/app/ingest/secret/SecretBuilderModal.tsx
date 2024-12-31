import { Button, Form, Input, Modal, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';
import { SecretBuilderState } from './types';

const NAME_FIELD_NAME = 'name';
const DESCRIPTION_FIELD_NAME = 'description';
const VALUE_FIELD_NAME = 'value';

type Props = {
    initialState?: SecretBuilderState;
    editSecret?: SecretBuilderState;
    open: boolean;
    onSubmit?: (source: SecretBuilderState, resetState: () => void) => void;
    onUpdate?: (source: SecretBuilderState, resetState: () => void) => void;
    onCancel?: () => void;
};

export const SecretBuilderModal = ({ initialState, editSecret, open, onSubmit, onUpdate, onCancel }: Props) => {
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [form] = Form.useForm();

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createSecretButton',
    });

    useEffect(() => {
        if (editSecret) {
            form.setFieldsValue({
                name: editSecret.name,
                description: editSecret.description,
                value: editSecret.value,
            });
        }
    }, [editSecret, form]);

    function resetValues() {
        setCreateButtonEnabled(false);
        form.resetFields();
    }

    const onCloseModal = () => {
        setCreateButtonEnabled(false);
        form.resetFields();
        onCancel?.();
    };

    const titleText = editSecret ? 'Edit Secret' : 'Create a new Secret';

    return (
        <Modal
            width={540}
            title={<Typography.Text>{titleText}</Typography.Text>}
            open={open}
            onCancel={onCloseModal}
            zIndex={1051} // one higher than other modals - needed for managed ingestion forms
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        Cancel
                    </Button>
                    <Button
                        data-testid="secret-modal-create-button"
                        id="createSecretButton"
                        onClick={() => {
                            if (!editSecret) {
                                onSubmit?.(
                                    {
                                        name: form.getFieldValue(NAME_FIELD_NAME),
                                        description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
                                        value: form.getFieldValue(VALUE_FIELD_NAME),
                                    },
                                    resetValues,
                                );
                            } else {
                                onUpdate?.(
                                    {
                                        urn: editSecret?.urn,
                                        name: form.getFieldValue(NAME_FIELD_NAME),
                                        description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
                                        value: form.getFieldValue(VALUE_FIELD_NAME),
                                    },
                                    resetValues,
                                );
                            }
                        }}
                        disabled={!createButtonEnabled}
                    >
                        {!editSecret ? 'Create' : 'Update'}
                    </Button>
                </>
            }
        >
            <Form
                form={form}
                initialValues={initialState}
                layout="vertical"
                onFieldsChange={() =>
                    setCreateButtonEnabled(!form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <Form.Item label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>
                        Give your secret a name. This is what you&apos;ll use to reference the secret from your recipes.
                    </Typography.Paragraph>
                    <Form.Item
                        data-testid="secret-modal-name-input"
                        name={NAME_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: 'Enter a name.',
                            },
                            { whitespace: false },
                            { min: 1, max: 50 },
                            {
                                pattern: /^[a-zA-Z_]+[a-zA-Z0-9_]*$/,
                                message:
                                    'Please start the secret name with a letter, followed by letters, digits, or underscores only.',
                            },
                        ]}
                        hasFeedback
                    >
                        <Input placeholder="A name for your secret" disabled={editSecret !== undefined} />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Value</Typography.Text>}>
                    <Typography.Paragraph>
                        The value of your secret, which will be encrypted and stored securely within DataHub.
                    </Typography.Paragraph>
                    <Form.Item
                        data-testid="secret-modal-value-input"
                        name={VALUE_FIELD_NAME}
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
                        <Input.TextArea placeholder="The value of your secret" autoComplete="false" />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>
                        An optional description to help keep track of your secret.
                    </Typography.Paragraph>
                    <Form.Item
                        data-testid="secret-modal-description-input"
                        name={DESCRIPTION_FIELD_NAME}
                        rules={[{ whitespace: true }, { min: 1, max: 500 }]}
                        hasFeedback
                    >
                        <Input.TextArea placeholder="A description for your secret" />
                    </Form.Item>
                </Form.Item>
            </Form>
        </Modal>
    );
};
