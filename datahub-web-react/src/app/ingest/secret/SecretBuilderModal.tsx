import { Button, Form, Input, Modal, Typography } from 'antd';
import React, { useState } from 'react';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';
import { SecretBuilderState } from './types';

const NAME_FIELD_NAME = 'name';
const DESCRIPTION_FIELD_NAME = 'description';
const VALUE_FIELD_NAME = 'value';

type Props = {
    initialState?: SecretBuilderState;
    visible: boolean;
    onSubmit?: (source: SecretBuilderState, resetState: () => void) => void;
    onCancel?: () => void;
};

export const SecretBuilderModal = ({ initialState, visible, onSubmit, onCancel }: Props) => {
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [form] = Form.useForm();

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createSecretButton',
    });

    function resetValues() {
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
                        onClick={() =>
                            onSubmit?.(
                                {
                                    name: form.getFieldValue(NAME_FIELD_NAME),
                                    description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
                                    value: form.getFieldValue(VALUE_FIELD_NAME),
                                },
                                resetValues,
                            )
                        }
                        disabled={!createButtonEnabled}
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
                    setCreateButtonEnabled(!form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <Form.Item label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>
                        Give your secret a name. This is what you&apos;ll use to reference the secret from your recipes.
                    </Typography.Paragraph>
                    <Form.Item
                        name={NAME_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: 'Enter a name.',
                            },
                            { whitespace: false },
                            { min: 1, max: 50 },
                            { pattern: /^[^\s\t${}\\,'"]+$/, message: 'This secret name is not allowed.' },
                        ]}
                        hasFeedback
                    >
                        <Input placeholder="A name for your secret" />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Value</Typography.Text>}>
                    <Typography.Paragraph>
                        The value of your secret, which will be encrypted and stored securely within DataHub.
                    </Typography.Paragraph>
                    <Form.Item
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
