import { Button, Divider, Modal, Select, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useGetAccessTokenLazyQuery } from '../../graphql/auth.generated';
import { AccessTokenDuration, AccessTokenType } from '../../types.generated';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';

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
const ModalSection = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;
const ModalSectionHeader = styled(Typography.Text)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 4px;
    }
`;
const ModalSectionParagraph = styled(Typography.Paragraph)`
    &&&& {
        padding: 0px;
        margin: 0px;
    }
`;

const ExpirationSelectConainer = styled.div`
    padding-top: 12px;
    padding-bottom: 12px;
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
    const canGeneratePersonalAccessTokens = authenticatedUser?.platformPrivileges?.viewAnalytics;
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
    const baseUrl = window.location.origin;
    const accessTokenCurl = `curl -X POST '${baseUrl}/api/graphql' \\
--header 'Authorization: Bearer ${accessToken}' \\
--header 'Content-Type: application/json' \\
--data-raw '{"query":"{\\n  me {\\n    corpUser {\\n        username\\n    }\\n  }\\n}","variables":{}}'`;

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
                    incorrect, please your DataHub administrator.
                </Typography.Paragraph>
            )}

            <Modal
                width={700}
                footer={null}
                title={
                    <Typography.Text>
                        Generate a new <b>Personal Access Token</b>
                    </Typography.Text>
                }
                visible={showModal}
                onCancel={onClose}
            >
                <ModalSection>
                    <ModalSectionHeader strong>Token</ModalSectionHeader>
                    <ModalSectionParagraph>
                        This token will expire in <b>{selectedExpiresInText}.</b>
                    </ModalSectionParagraph>
                    <Typography.Paragraph copyable={{ text: accessToken }}>
                        <pre>{accessToken}</pre>
                    </Typography.Paragraph>
                </ModalSection>
                <ModalSection>
                    <ModalSectionHeader strong>Usage</ModalSectionHeader>
                    <ModalSectionParagraph>
                        To use the token, provide it as a <Typography.Text keyboard>Bearer</Typography.Text> token in
                        the <Typography.Text keyboard>Authorization</Typography.Text> header when making API requests:
                    </ModalSectionParagraph>
                    <Typography.Paragraph copyable={{ text: accessTokenCurl }}>
                        <pre>{accessTokenCurl}</pre>
                    </Typography.Paragraph>
                </ModalSection>
                <ModalSection>
                    <ModalSectionHeader strong>Learn More</ModalSectionHeader>
                    <ModalSectionParagraph>
                        To learn more about the DataHub APIs, check out the
                        <a href="https://www.datahubproject.io/docs/"> DataHub Docs.</a>
                    </ModalSectionParagraph>
                </ModalSection>
            </Modal>
        </ContentContainer>
    );
};
