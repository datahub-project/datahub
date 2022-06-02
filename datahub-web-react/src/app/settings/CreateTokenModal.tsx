import React, { useState } from 'react';
import { message, Button, Input, Modal, Typography, Form, Select } from 'antd';
import styled from 'styled-components';

import { useEnterKeyListener } from '../shared/useEnterKeyListener';
import { ACCESS_TOKEN_DURATIONS, getTokenExpireDate } from './utils';
import { useCreateAccessTokenMutation } from '../../graphql/auth.generated';
import { AccessTokenDuration, AccessTokenType, CreateAccessTokenInput } from '../../types.generated';

type Props = {
    currentUserUrn: string;
    visible: boolean;
    onClose: () => void;
    onCreate: () => void;
};

const ExpirationSelectContainer = styled.div`
    padding: 1px;
`;

const ExpirationDurationSelect = styled(Select)`
    && {
        width: 100%;
        margin-top: 1em;
        margin-bottom: 1em;
    }
`;

export default function CreateTokenModal({ currentUserUrn, visible, onClose, onCreate }: Props) {
    const [tokenName, setTokenName] = useState('');
    const [tokenDescription, setTokenDescription] = useState('');
    const [isTokenDurationChanged, setIsTokenDurationChanged] = useState(false);
    const [selectedTokenDuration, setSelectedTokenDuration] = useState(ACCESS_TOKEN_DURATIONS[2].duration);

    const [createAccessToken] = useCreateAccessTokenMutation();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(true);
    const [form] = Form.useForm();

    const onModalClose = () => {
        setTokenName('');
        setTokenDescription('');
        setSelectedTokenDuration(ACCESS_TOKEN_DURATIONS[2].duration);
        setIsTokenDurationChanged(false);
        form.resetFields();
        onClose();
    };

    const onCreateToken = () => {
        const input: CreateAccessTokenInput = {
            actorUrn: currentUserUrn,
            type: AccessTokenType.Personal,
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
                    content: `Token Created Successfully!`,
                    duration: 3,
                });
                onCreate();
            });
        onModalClose();
    };

    // Function to handle the selection of Token Duration
    const onSelectTokenDurationHandler = (duration: AccessTokenDuration) => {
        setIsTokenDurationChanged(true);
        setSelectedTokenDuration(duration);
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createTokenButton',
    });

    return (
        <Modal
            title="Create new Token"
            visible={visible}
            onCancel={onModalClose}
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
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
                <ExpirationSelectContainer>
                    <Typography.Text strong>Expires in</Typography.Text>
                    <ExpirationDurationSelect
                        value={selectedTokenDuration}
                        onSelect={(duration) => onSelectTokenDurationHandler(duration as AccessTokenDuration)}
                    >
                        {ACCESS_TOKEN_DURATIONS.map((duration) => (
                            <Select.Option key={duration.text} value={duration.duration}>
                                {duration.text}
                            </Select.Option>
                        ))}
                    </ExpirationDurationSelect>
                    {isTokenDurationChanged && (
                        <Typography.Text>{getTokenExpireDate(selectedTokenDuration)}</Typography.Text>
                    )}
                </ExpirationSelectContainer>
            </Form>
        </Modal>
    );
}
