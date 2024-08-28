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
    open: boolean;
    onClose: () => void;
    onCreateToken: () => void;
};

type FormProps = {
    name: string;
    description?: string;
    duration: AccessTokenDuration;
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

export default function CreateTokenModal({ currentUserUrn, open, onClose, onCreateToken }: Props) {
    const [selectedTokenDuration, setSelectedTokenDuration] = useState<AccessTokenDuration | null>(null);

    const [showModal, setShowModal] = useState(false);
    const [createButtonEnabled, setCreateButtonEnabled] = useState(true);

    const [createAccessToken, { data }] = useCreateAccessTokenMutation();

    const [form] = Form.useForm<FormProps>();

    // Check and show the modal once the data for createAccessToken will generate
    useEffect(() => {
        if (data && data.createAccessToken?.accessToken) {
            setShowModal(true);
        }
    }, [data, setShowModal]);

    // Function to handle the close or cross button of Access Token Modal
    const onDetailModalClose = () => {
        setSelectedTokenDuration(null);
        setShowModal(false);
        onClose();
    };

    // Function to handle the close or cross button of Create Token Modal
    const onModalClose = () => {
        form.resetFields();
        onClose();
    };

    const onCreateNewToken = () => {
        const { duration, name, description } = form.getFieldsValue();
        const input: CreateAccessTokenInput = {
            actorUrn: currentUserUrn,
            type: AccessTokenType.Personal,
            duration,
            name,
            description,
        };
        createAccessToken({ variables: { input } })
            .then(({ errors }) => {
                if (!errors) {
                    setSelectedTokenDuration(duration);
                    analytics.event({
                        type: EventType.CreateAccessTokenEvent,
                        accessTokenType: AccessTokenType.Personal,
                        duration,
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
    const selectedExpiresInText = selectedTokenDuration && getTokenExpireDate(selectedTokenDuration);

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createTokenButton',
    });

    const hasSelectedNoExpiration = selectedTokenDuration === AccessTokenDuration.NoExpiry;

    return (
        <>
            <Modal
                title="Create new Token"
                open={open}
                onCancel={onModalClose}
                footer={
                    <>
                        <Button onClick={onModalClose} type="text" data-testid="cancel-create-access-token-button">
                            Cancel
                        </Button>
                        <Button
                            id="createTokenButton"
                            onClick={onCreateNewToken}
                            disabled={createButtonEnabled}
                            data-testid="create-access-token-button"
                        >
                            Create
                        </Button>
                    </>
                }
            >
                <Form
                    form={form}
                    initialValues={{ duration: ACCESS_TOKEN_DURATIONS[2].duration }}
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
                            <Input placeholder="A name for your token" data-testid="create-access-token-name" />
                        </Form.Item>
                    </Form.Item>
                    <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                        <Typography.Paragraph>An optional description for your new token.</Typography.Paragraph>
                        <Form.Item name="description" rules={[{ whitespace: true }, { min: 1, max: 500 }]} hasFeedback>
                            <Input
                                placeholder="A description for your token"
                                data-testid="create-access-token-description"
                            />
                        </Form.Item>
                    </Form.Item>
                    <ExpirationSelectContainer>
                        <Typography.Text strong>Expires in</Typography.Text>
                        <Form.Item name="duration" data-testid="create-access-token-duration" noStyle>
                            <ExpirationDurationSelect>
                                {ACCESS_TOKEN_DURATIONS.map((duration) => (
                                    <Select.Option key={duration.text} value={duration.duration}>
                                        <OptionText isRed={duration.duration === AccessTokenDuration.NoExpiry}>
                                            {duration.text}
                                        </OptionText>
                                    </Select.Option>
                                ))}
                            </ExpirationDurationSelect>
                        </Form.Item>
                        <Form.Item shouldUpdate={(prev, cur) => prev.duration !== cur.duration} noStyle>
                            {({ getFieldValue }) => (
                                <Typography.Text
                                    type="secondary"
                                    style={hasSelectedNoExpiration ? { color: `${red[5]}` } : {}}
                                >
                                    {getFieldValue('duration') && getTokenExpireDate(getFieldValue('duration'))}
                                </Typography.Text>
                            )}
                        </Form.Item>
                    </ExpirationSelectContainer>
                </Form>
            </Modal>
            <AccessTokenModal
                open={showModal}
                onClose={onDetailModalClose}
                accessToken={accessToken || ''}
                expiresInText={selectedExpiresInText || ''}
            />
        </>
    );
}
