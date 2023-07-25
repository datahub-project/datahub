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
            title={<Typography.Text>创建密钥</Typography.Text>}
            visible={visible}
            onCancel={onCancel}
            zIndex={1051} // one higher than other modals - needed for managed ingestion forms
            footer={
                <>
                    <Button onClick={onCancel} type="text">
                        取消
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
                        创建
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
                <Form.Item label={<Typography.Text strong>名称</Typography.Text>}>
                    <Typography.Paragraph>
                        密钥名称. This is what you&apos;ll use to reference the secret from your recipes.
                    </Typography.Paragraph>
                    <Form.Item
                        name={NAME_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: '输入名称.',
                            },
                            { whitespace: false },
                            { min: 1, max: 50 },
                            { pattern: /^[^\s\t${}\\,'"]+$/, message: '不允许的密钥名称.' },
                        ]}
                        hasFeedback
                    >
                        <Input placeholder="密钥名称" />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>密钥</Typography.Text>}>
                    <Typography.Paragraph>
                        密钥具体的值，该密钥会以加密的形式保存在DataHub中.
                    </Typography.Paragraph>
                    <Form.Item
                        name={VALUE_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: '输入密钥.',
                            },
                            // { whitespace: true },
                            { min: 1 },
                        ]}
                        hasFeedback
                    >
                        <Input.TextArea placeholder="密钥" autoComplete="false" />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>描述</Typography.Text>}>
                    <Typography.Paragraph>
                        密钥的描述（可选）.
                    </Typography.Paragraph>
                    <Form.Item
                        name={DESCRIPTION_FIELD_NAME}
                        rules={[{ whitespace: true }, { min: 1, max: 500 }]}
                        hasFeedback
                    >
                        <Input.TextArea placeholder="密钥的描述" />
                    </Form.Item>
                </Form.Item>
            </Form>
        </Modal>
    );
};
