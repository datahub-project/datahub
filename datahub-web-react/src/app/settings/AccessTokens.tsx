import React, { useState, useMemo } from 'react';
import styled from 'styled-components';
import { Alert, Button, Divider, Empty, message, Modal, Pagination, Typography } from 'antd';
import { DeleteOutlined, InfoCircleOutlined, PlusOutlined } from '@ant-design/icons';

import { FacetFilterInput } from '../../types.generated';
import { useListAccessTokensQuery, useRevokeAccessTokenMutation } from '../../graphql/auth.generated';
import { Message } from '../shared/Message';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import { StyledTable } from '../entity/shared/components/styled/StyledTable';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import CreateTokenModal from './CreateTokenModal';
import { useAppConfigQuery } from '../../graphql/app.generated';
import { getLocaleTimezone } from '../shared/time/timeUtils';

const SourceContainer = styled.div`
    width: 100%;
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
`;

const TokensContainer = styled.div`
    padding-top: 0px;
`;

const TokensHeaderContainer = styled.div`
    && {
        padding-left: 0px;
    }
`;

const TokensTitle = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
`;

const StyledAlert = styled(Alert)`
    padding-top: 12px;
    padding-bottom: 12px;
    margin-bottom: 20px;
`;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    margin-right: 8px;
`;

const PersonTokenDescriptionText = styled(Typography.Paragraph)`
    && {
        max-width: 700px;
        margin-top: 12px;
        margin-bottom: 16px;
    }
`;

const ActionButtonContainer = styled.div`
    display: flex;
    justify-content: right;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const DEFAULT_PAGE_SIZE = 10;

export const AccessTokens = () => {
    const [isCreatingToken, setIsCreatingToken] = useState(false);
    const [removedTokens, setRemovedTokens] = useState<string[]>([]);

    // Current User Urn
    const authenticatedUser = useGetAuthenticatedUser();
    const currentUserUrn = authenticatedUser?.corpUser.urn || '';

    const isTokenAuthEnabled = useAppConfigQuery().data?.appConfig?.authConfig?.tokenAuthEnabled;
    const canGeneratePersonalAccessTokens =
        isTokenAuthEnabled && authenticatedUser?.platformPrivileges.generatePersonalAccessTokens;

    // Access Tokens list paging.
    const [page, setPage] = useState(1);
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    // Filters for Access Tokens list
    const filters: Array<FacetFilterInput> = [
        {
            field: 'ownerUrn',
            value: currentUserUrn,
        },
    ];

    // Call list Access Token Mutation
    const {
        loading: tokensLoading,
        error: tokensError,
        data: tokensData,
        refetch: tokensRefetch,
    } = useListAccessTokensQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                filters,
            },
        },
    });

    const totalTokens = tokensData?.listAccessTokens.total || 0;
    const tokens = useMemo(() => tokensData?.listAccessTokens.tokens || [], [tokensData]);
    const filteredTokens = tokens.filter((token) => !removedTokens.includes(token.id));

    // Any time a access token  is removed or created, refetch the list.
    const [revokeAccessToken, { error: revokeTokenError }] = useRevokeAccessTokenMutation();

    // Revoke token Handler
    const onRemoveToken = (token: any) => {
        Modal.confirm({
            title: 'Are you sure you want to revoke this token?',
            content: `Anyone using this token will no longer be able to access the DataHub API. You cannot undo this action.`,
            onOk() {
                // Hack to deal with eventual consistency.
                const newTokenIds = [...removedTokens, token.id];
                setRemovedTokens(newTokenIds);
                revokeAccessToken({ variables: { tokenId: token.id } })
                    .catch((e) => {
                        message.destroy();
                        message.error({ content: `Failed to revoke Token!: \n ${e.message || ''}`, duration: 3 });
                    })
                    .finally(() => {
                        setTimeout(function () {
                            tokensRefetch?.();
                        }, 3000);
                    });
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const tableData = filteredTokens?.map((token) => ({
        urn: token.urn,
        type: token.type,
        id: token.id,
        name: token.name,
        description: token.description,
        actorUrn: token.actorUrn,
        ownerUrn: token.ownerUrn,
        createdAt: token.createdAt,
        expiresAt: token.expiresAt,
    }));

    const tableColumns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (name: string) => <b>{name}</b>,
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            render: (description: string) => description || '',
        },
        {
            title: 'Expires At',
            dataIndex: 'expiresAt',
            key: 'expiresAt',
            render: (expiresAt: string) => {
                const localeTimezone = getLocaleTimezone();
                const formattedExpireAt = new Date(expiresAt);
                return (
                    <span>{`${formattedExpireAt.toLocaleDateString()} at ${formattedExpireAt.toLocaleTimeString()} (${localeTimezone})`}</span>
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <ActionButtonContainer>
                    <Button onClick={() => onRemoveToken(record)} icon={<DeleteOutlined />} danger>
                        Revoke
                    </Button>
                </ActionButtonContainer>
            ),
        },
    ];

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    return (
        <SourceContainer>
            {tokensLoading && !tokensData && (
                <Message type="loading" content="Loading tokens..." style={{ marginTop: '10%' }} />
            )}
            {tokensError && message.error('Failed to load tokens :(')}
            {revokeTokenError && message.error('Failed to update the Token :(')}
            <TokensContainer>
                <TokensHeaderContainer>
                    <TokensTitle level={2}>Manage Access Tokens</TokensTitle>
                    <Typography.Paragraph type="secondary">
                        Manage Access Tokens for use with DataHub APIs.
                    </Typography.Paragraph>
                </TokensHeaderContainer>
            </TokensContainer>
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
                privileges and have a finite lifespan. Do not share Personal Access Tokens.
            </PersonTokenDescriptionText>
            <TabToolbar>
                <div>
                    <Button
                        type="text"
                        onClick={() => setIsCreatingToken(true)}
                        data-testid="add-token-button"
                        disabled={!canGeneratePersonalAccessTokens}
                    >
                        <PlusOutlined /> Generate new token
                    </Button>
                </div>
            </TabToolbar>
            <StyledTable
                columns={tableColumns}
                dataSource={tableData}
                rowKey="urn"
                locale={{
                    emptyText: <Empty description="No Access Tokens!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                pagination={false}
            />
            <PaginationContainer>
                <Pagination
                    style={{ margin: 40 }}
                    current={page}
                    pageSize={pageSize}
                    total={totalTokens}
                    showLessItems
                    onChange={onChangePage}
                    showSizeChanger={false}
                />
            </PaginationContainer>
            <CreateTokenModal
                currentUserUrn={currentUserUrn}
                visible={isCreatingToken}
                onClose={() => setIsCreatingToken(false)}
                onCreateToken={() => {
                    // Hack to deal with eventual consistency.
                    setTimeout(function () {
                        tokensRefetch?.();
                    }, 3000);
                }}
            />
        </SourceContainer>
    );
};
