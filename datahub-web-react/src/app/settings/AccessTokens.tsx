import { InfoCircleOutlined } from '@ant-design/icons';
import { Alert, Button, Divider, Select, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useAppConfigQuery } from '../../graphql/app.generated';
import { useGetAccessTokenLazyQuery } from '../../graphql/auth.generated';
import { AccessTokenDuration, AccessTokenType } from '../../types.generated';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { AccessTokenModal } from './AccessTokenModal';

const ContentContainer = styled.div`
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
    width: 100%;
`;

const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
`;

const PersonTokenDescriptionText = styled(Typography.Paragraph)`
    && {
        max-width: 700px;
        margin-top: 12px;
        margin-bottom: 16px;
    }
`;

const ExpirationSelectConainer = styled.div`
    padding-top: 12px;
    padding-bottom: 12px;
`;

const StyledAlert = styled(Alert)`
    padding-top: 12px;
    padding-bottom: 12px;
    margin-bottom: 20px;
`;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    margin-right: 8px;
`;

const ExpirationDurationSelect = styled(Select)`
    && {
        width: 120px;
        margin-right: 20px;
    }
`;

const ACCESS_TOKEN_DURATIONS = [
    { text: '1 hour', duration: AccessTokenDuration.OneHour },
    { text: '1 day', duration: AccessTokenDuration.OneDay },
    { text: '1 month', duration: AccessTokenDuration.OneMonth },
    { text: '3 months', duration: AccessTokenDuration.ThreeMonths },
    { text: '6 months', duration: AccessTokenDuration.SixMonths },
];

export const AccessTokens = () => {
    const [showModal, setShowModal] = useState(false);
    const [selectedTokenDuration, setSelectedTokenDuration] = useState(ACCESS_TOKEN_DURATIONS[0].duration);
    const authenticatedUser = useGetAuthenticatedUser();
    const isTokenAuthEnabled = useAppConfigQuery().data?.appConfig?.authConfig?.tokenAuthEnabled;
    const canGeneratePersonalAccessTokens =
        isTokenAuthEnabled && authenticatedUser?.platformPrivileges.generatePersonalAccessTokens;
    const currentUserUrn = authenticatedUser?.corpUser.urn;

    const [getAccessToken, { data, error }] = useGetAccessTokenLazyQuery({
        fetchPolicy: 'no-cache',
    });

    useEffect(() => {
        if (data?.getAccessToken?.accessToken) {
            setShowModal(true);
        }
    }, [data?.getAccessToken?.accessToken, setShowModal]);

    const generateAccessToken = () => {
        if (currentUserUrn) {
            getAccessToken({
                variables: {
                    input: {
                        type: AccessTokenType.Personal,
                        actorUrn: currentUserUrn,
                        duration: selectedTokenDuration,
                    },
                },
            });
        }
    };

    const onClose = () => {
        setShowModal(false);
    };

    const accessToken = data?.getAccessToken?.accessToken;
    const selectedExpiresInText = ACCESS_TOKEN_DURATIONS.find(
        (duration) => duration.duration === selectedTokenDuration,
    )?.text;

    return (
        <ContentContainer>
            <PageTitle level={3}>Access Tokens</PageTitle>
            <Typography.Paragraph type="secondary">
                Manage Access Tokens for use with DataHub APIs.
            </Typography.Paragraph>
            <Divider />
            {isTokenAuthEnabled === false && (
                <StyledAlert
                    type="error"
                    message={
                        <span>
                            <StyledInfoCircleOutlined />
                            Token based authentication is currently disabled. Contact your DataHub administrator to
                            enable this feature.
                        </span>
                    }
                />
            )}
            <Typography.Title level={5}>Personal Access Tokens</Typography.Title>
            <PersonTokenDescriptionText type="secondary">
                Personal Access Tokens allow you to make programmatic requests to DataHub&apos;s APIs. They inherit your
                privileges and have a finite lifespan. Do not share Personal Access Tokens. Once generated, they cannot
                easily be revoked.
            </PersonTokenDescriptionText>
            {error && (
                <Typography.Paragraph type="danger">
                    Failed to generate token. Please contact your DataHub administrator.
                </Typography.Paragraph>
            )}
            <Typography.Text strong>Expires in</Typography.Text>
            <ExpirationSelectConainer>
                <span>
                    <ExpirationDurationSelect
                        value={selectedTokenDuration}
                        onSelect={(duration) => setSelectedTokenDuration(duration as AccessTokenDuration)}
                    >
                        {ACCESS_TOKEN_DURATIONS.map((duration) => (
                            <Select.Option key={duration.text} value={duration.duration}>
                                {duration.text}
                            </Select.Option>
                        ))}
                    </ExpirationDurationSelect>
                    <Button
                        disabled={!canGeneratePersonalAccessTokens}
                        onClick={() => {
                            generateAccessToken();
                        }}
                    >
                        Generate Personal Access Token
                    </Button>
                </span>
            </ExpirationSelectConainer>
            {!canGeneratePersonalAccessTokens && (
                <Typography.Paragraph type="secondary">
                    Looks like you are not authorized to generate Personal Access Tokens. If you think this is
                    incorrect, please contact your DataHub administrator.
                </Typography.Paragraph>
            )}
            <AccessTokenModal
                visible={showModal}
                onClose={onClose}
                accessToken={accessToken || ''}
                expiresInText={selectedExpiresInText || ''}
            />
        </ContentContainer>
    );
};
