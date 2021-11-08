import { Button, Divider, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useGetAccessTokenLazyQuery } from '../../graphql/auth.generated';
import { AccessTokenDuration, AccessTokenType } from '../../types.generated';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';

const ContentContainer = styled.div`
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
`;
const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
`;
const PersonTokenDescriptionText = styled(Typography.Paragraph)`
    && {
        margin-top: 12px;
        margin-bottom: 12px;
    }
`;

export const AccessTokens = () => {
    const [getAccessToken, { data, loading, error }] = useGetAccessTokenLazyQuery({
        fetchPolicy: 'no-cache',
    });
    console.log(loading);
    console.log(error); // TODO
    const currentUserUrn = useGetAuthenticatedUser()?.corpUser?.urn;

    const generateAccessToken = () => {
        if (currentUserUrn) {
            // TODO: Make expiration configurable.
            getAccessToken({
                variables: {
                    input: {
                        type: AccessTokenType.Personal,
                        actorUrn: currentUserUrn,
                        duration: AccessTokenDuration.OneMonth, // Default to one month.
                    },
                },
            });
        }
    };

    return (
        <ContentContainer>
            <PageTitle level={3}>Access Tokens</PageTitle>
            <Typography.Paragraph type="secondary">
                Manage access tokens for use with DataHub APIs.
            </Typography.Paragraph>
            <Divider />
            <Button
                onClick={() => {
                    generateAccessToken();
                }}
            >
                Generate Personal Access Token
            </Button>
            <PersonTokenDescriptionText type="secondary">
                {
                    "With a personal access token, you'll be able to make programmatic requests to DataHub's APIs by placing the token in the `Authorization` header as `Authorization: Bearer <token>`"
                }
                This type of token token will inherit your DataHub privileges and will be valid for 30 days.
            </PersonTokenDescriptionText>
            {data?.getAccessToken?.accessToken && (
                <Typography.Paragraph copyable style={{ width: 300 }}>
                    <pre>{data?.getAccessToken?.accessToken}</pre>
                </Typography.Paragraph>
            )}
        </ContentContainer>
    );
};
