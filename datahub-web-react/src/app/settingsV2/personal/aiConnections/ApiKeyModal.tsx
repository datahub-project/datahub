import { KeyOutlined } from '@ant-design/icons';
import { Form, Input, Modal } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { colors } from '@src/alchemy-components';

const Description = styled.p`
    font-size: 14px;
    color: ${colors.gray[1700]};
    margin: 0 0 16px 0;
    line-height: 1.5;
`;

const SecurityNote = styled.div`
    font-size: 12px;
    color: ${colors.gray[500]};
    background: ${colors.gray[100]};
    padding: 12px;
    border-radius: 6px;
    margin-top: 16px;
`;

interface ApiKeyModalProps {
    /** Whether the modal is visible */
    open: boolean;
    /** Plugin display name */
    pluginName: string;
    /** Callback when modal is closed */
    onClose: () => void;
    /** Callback when API key is submitted */
    onSubmit: (apiKey: string) => Promise<void>;
}

/**
 * Modal for entering a personal API key for an AI plugin.
 */
const ApiKeyModal: React.FC<ApiKeyModalProps> = ({ open, pluginName, onClose, onSubmit }) => {
    const [form] = Form.useForm();
    const [isSubmitting, setIsSubmitting] = useState(false);

    const handleSubmit = async () => {
        try {
            const values = await form.validateFields();
            setIsSubmitting(true);
            await onSubmit(values.apiKey);
            form.resetFields();
            onClose();
        } catch (error) {
            // Form validation failed or submission error
            // Error message is handled by the parent component
        } finally {
            setIsSubmitting(false);
        }
    };

    const handleCancel = () => {
        form.resetFields();
        onClose();
    };

    return (
        <Modal
            title={
                <>
                    <KeyOutlined style={{ marginRight: 8 }} />
                    Connect to {pluginName}
                </>
            }
            open={open}
            onOk={handleSubmit}
            onCancel={handleCancel}
            okText="Connect"
            okButtonProps={{ loading: isSubmitting }}
            destroyOnClose
        >
            <Description>
                Enter your personal API key for {pluginName}. This key will be stored securely and used when you
                interact with this plugin through Ask DataHub.
            </Description>

            <Form form={form} layout="vertical">
                <Form.Item
                    name="apiKey"
                    label="API Key"
                    rules={[
                        { required: true, message: 'Please enter your API key' },
                        { min: 8, message: 'API key seems too short' },
                    ]}
                >
                    <Input.Password
                        placeholder="Enter your API key"
                        prefix={<KeyOutlined style={{ color: colors.gray[400] }} />}
                        autoComplete="off"
                    />
                </Form.Item>
            </Form>

            <SecurityNote>
                🔒 Your API key is encrypted and stored securely. It is only accessible to you and is never shared with
                other users or exposed in logs.
            </SecurityNote>
        </Modal>
    );
};

export default ApiKeyModal;
