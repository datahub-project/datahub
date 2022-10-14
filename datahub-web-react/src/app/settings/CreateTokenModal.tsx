import React, { useState, useEffect } from 'react';
import { message, Button, Input, Modal, Typography, Form, Select } from 'antd';
import styled from 'styled-components';
import { red } from '@ant-design/colors';

import { useEnterKeyListener } from '../shared/useEnterKeyListener';
import { ACCESS_TOKEN_DURATIONS, getTokenExpireDate } from './utils';
import { useCreateAccessTokenMutation } from '../../graphql/auth.generated';
import { AccessTokenDuration, AccessTokenType, CreateAccessTokenInput } from '../../types.generated';
import { AccessTokenModal } from './AccessTokenModal';
import analytics, { EventType } from '../analytics';

type Props = {
    currentUserUrn: string;
    visible: boolean;
    onClose: () => void;
    onCreateToken: () => void;
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

const OptionText = styled.span<{ isRed: boolean }>`
    ${(props) => props.isRed && `color: ${red[5]};`}
`;

export default function CreateTokenModal({ currentUserUrn, visible, onClose, onCreateToken }: Props) {
    const [tokenName, setTokenName] = useState('');
    const [tokenDescription, setTokenDescription] = useState('');
    const [selectedTokenDuration, setSelectedTokenDuration] = useState(ACCESS_TOKEN_DURATIONS[2].duration);

    const [showModal, setShowModal] = useState(false);
    const [createButtonEnabled, setCreateButtonEnabled] = useState(true);

    const [createAccessToken, { data }] = useCreateAccessTokenMutation();

    const [form] = Form.useForm();

    // Check and show the modal once the data for createAccessToken will generate
    useEffect(() => {
        if (data && data.createAccessToken?.accessToken) {
            setShowModal(true);
        }
    }, [data, setShowModal]);

    // Function to handle the close or cross button of Access Token Modal
    const onDetailModalClose = () => {
        setShowModal(false);
        onClose();
    };

    // Function to handle the close or cross button of Create Token Modal
    const onModalClose = () => {
        setTokenName('');
        setTokenDescription('');
        setSelectedTokenDuration(ACCESS_TOKEN_DURATIONS[2].duration);
        form.resetFields();
        onClose();
    };

    const onCreateNewToken = () => {
        const input: CreateAccessTokenInput = {
            actorUrn: currentUserUrn,
            type: AccessTokenType.Personal,
            duration: selectedTokenDuration,
            name: tokenName,
            description: tokenDescription,
        };
        createAccessToken({ variables: { input } })
            .then(({ errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.CreateAccessTokenEvent,
                        accessTokenType: AccessTokenType.Personal,
                        duration: selectedTokenDuration,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create Token!: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                onCreateToken();
            });
        onModalClose();
    };

    const accessToken = data && data.createAccessToken?.accessToken;
    const selectedExpiresInText = getTokenExpireDate(selectedTokenDuration);

    // Function to handle the selection of Token Duration
    const onSelectTokenDurationHandler = (duration: AccessTokenDuration) => {
        setSelectedTokenDuration(duration);
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createTokenButton',
    });

    const hasSelectedNoExpiration = selectedTokenDuration === AccessTokenDuration.NoExpiry;

    return (
        <>
            <Modal
                title="Create new Token"
                visible={visible}
                onCancel={onModalClose}
                footer={
                    <>
                        <Button onClick={onModalClose} type="text">
                            Cancel
                        </Button>
                        <Button id="createTokenButton" onClick={onCreateNewToken} disabled={createButtonEnabled}>
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
                        <Typography.Paragraph>Give your new token a name. </Typography.Paragraph>
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
                                    <OptionText isRed={duration.duration === AccessTokenDuration.NoExpiry}>
                                        {duration.text}
                                    </OptionText>
                                </Select.Option>
                            ))}
                        </ExpirationDurationSelect>
                        <Typography.Text type="secondary" style={hasSelectedNoExpiration ? { color: `${red[5]}` } : {}}>
                            {getTokenExpireDate(selectedTokenDuration)}
                        </Typography.Text>
                    </ExpirationSelectContainer>
                </Form>
            </Modal>
            <AccessTokenModal
                visible={showModal}
                onClose={onDetailModalClose}
                accessToken={accessToken || ''}
                expiresInText={selectedExpiresInText || ''}
            />
        </>
    );
}
