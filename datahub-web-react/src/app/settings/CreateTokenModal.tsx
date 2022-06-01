import React, { useState } from 'react';
import { message, Button, Input, Modal, Typography, Form, Select } from 'antd';

import { useEnterKeyListener } from '../shared/useEnterKeyListener';
import { ACCESS_TOKEN_DURATIONS, ACCESS_TOKEN_TYPES } from './utils';
import { useCreateAccessTokenMutation } from '../../graphql/auth.generated';
import { AccessTokenDuration, AccessTokenType, CreateAccessTokenInput } from '../../types.generated';

type Props = {
    currentUserUrn: string;
    visible: boolean;
    onClose: () => void;
    onCreate: () => void;
};

export default function CreateTokenModal({ currentUserUrn, visible, onClose, onCreate }: Props) {
    const [tokenName, setTokenName] = useState('');
    const [tokenDescription, setTokenDescription] = useState('');
    const [selectedTokenDuration, setSelectedTokenDuration] = useState(ACCESS_TOKEN_DURATIONS[0].duration);
    const [selectedTokenType, setSelectedTokenType] = useState(ACCESS_TOKEN_TYPES[0].type);

    const [createAccessToken] = useCreateAccessTokenMutation();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(true);
    const [form] = Form.useForm();

    const onCreateToken = () => {
        const input: CreateAccessTokenInput = {
            actorUrn: currentUserUrn,
            type: selectedTokenType,
            duration: selectedTokenDuration,
            name: tokenName,
            description: tokenDescription,
        };
        createAccessToken({ variables: { input } })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create Token!: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.success({
                    content: `Created Token!`,
                    duration: 3,
                });
                onCreate();
                setTokenName('');
                setTokenDescription('');
                setSelectedTokenDuration(ACCESS_TOKEN_DURATIONS[0].duration);
                setSelectedTokenType(ACCESS_TOKEN_TYPES[0].type);
                form.resetFields();
            });
        onClose();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createTokenButton',
    });

    return (
        <Modal
            title="Create new Token"
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button id="createTokenButton" onClick={onCreateToken} disabled={createButtonEnabled}>
                        Create
                    </Button>
                </>
            }
        >
            <Form
                form={form}
                initialValues={{}}
                layout="vertical"
                onFieldsChange={() =>
                    setCreateButtonEnabled(form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <Form.Item label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>Give your new Token a name. </Typography.Paragraph>
                    <Form.Item
                        name="name"
                        rules={[
                            {
                                required: true,
                                message: 'Enter a token name.',
                            },
                            { whitespace: true },
                            { min: 1, max: 50 },
                        ]}
                        hasFeedback
                    >
                        <Input
                            placeholder="A name for your token"
                            value={tokenName}
                            onChange={(event) => setTokenName(event.target.value)}
                        />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>An optional description for your new token.</Typography.Paragraph>
                    <Form.Item name="description" rules={[{ whitespace: true }, { min: 1, max: 500 }]} hasFeedback>
                        <Input
                            placeholder="A description for your token"
                            value={tokenDescription}
                            onChange={(event) => setTokenDescription(event.target.value)}
                        />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Type</Typography.Text>}>
                    <Form.Item name="type">
                        <Select
                            value={selectedTokenType}
                            onSelect={(type) => setSelectedTokenType(type as AccessTokenType)}
                        >
                            {ACCESS_TOKEN_TYPES.map((type) => (
                                <Select.Option key={type.text} value={type.type}>
                                    {type.text}
                                </Select.Option>
                            ))}
                        </Select>
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Expire in</Typography.Text>}>
                    <Form.Item name="expire_in">
                        <Select
                            value={selectedTokenDuration}
                            onSelect={(duration) => setSelectedTokenDuration(duration as AccessTokenDuration)}
                        >
                            {ACCESS_TOKEN_DURATIONS.map((duration) => (
                                <Select.Option key={duration.text} value={duration.duration}>
                                    {duration.text}
                                </Select.Option>
                            ))}
                        </Select>
                    </Form.Item>
                </Form.Item>
            </Form>
        </Modal>
    );
}
