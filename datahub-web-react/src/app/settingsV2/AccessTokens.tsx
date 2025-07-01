import { red } from '@ant-design/colors';
import { DeleteOutlined, InfoCircleOutlined, PlusOutlined } from '@ant-design/icons';
import { Alert, Button, Divider, Dropdown, Empty, Modal, Pagination, Select, Typography, message } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { StyledTable } from '@app/entity/shared/components/styled/StyledTable';
import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import CreateTokenModal from '@app/settingsV2/CreateTokenModal';
import { Message } from '@app/shared/Message';
import { OwnerLabel } from '@app/shared/OwnerLabel';
import { scrollToTop } from '@app/shared/searchUtils';
import { getLocaleTimezone } from '@app/shared/time/timeUtils';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListAccessTokensQuery, useRevokeAccessTokenMutation } from '@graphql/auth.generated';
import { useListUsersQuery } from '@graphql/user.generated';
import { EntityType, FacetFilterInput } from '@types';

const SourceContainer = styled.div`
    width: 100%;
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
    display: flex;
    flex-direction: column;
    overflow: auto;
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

const StyledSelectOwner = styled(Select)`
    margin-right: 15px;
    width: 200px;
`;

const StyledSelect = styled(Select)`
    margin-right: 15px;
    min-width: 75px;
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

const NeverExpireText = styled.span`
    color: ${red[5]};
`;

const SelectContainer = styled.div`
    display: flex;
    align-items: flex-start;
`;

const DEFAULT_PAGE_SIZE = 10;

export enum StatusType {
    ALL,
    EXPIRED,
}

export const AccessTokens = () => {
    const [createTokenFor, setCreateTokenFor] = useState<'personal' | 'remote-executor' | undefined>(undefined);
    const [removedTokens, setRemovedTokens] = useState<string[]>([]);
    const [statusFilter, setStatusFilter] = useState(StatusType.ALL);
    const [owner, setOwner] = useState('All');
    const [filters, setFilters] = useState<Array<FacetFilterInput> | null>(null);
    const [query, setQuery] = useState<undefined | string>(undefined);
    // Current User Urn
    const authenticatedUser = useUserContext();
    const entityRegistry = useEntityRegistry();
    const currentUserUrn = authenticatedUser?.user?.urn || '';

    useEffect(() => {
        if (currentUserUrn) {
            setFilters([
                {
                    field: 'ownerUrn',
                    values: [currentUserUrn],
                },
            ]);
        }
    }, [currentUserUrn]);

    const isTokenAuthEnabled = useAppConfig().config?.authConfig?.tokenAuthEnabled;
    const canGeneratePersonalAccessTokens =
        isTokenAuthEnabled && authenticatedUser?.platformPrivileges?.generatePersonalAccessTokens;

    const canManageToken = authenticatedUser?.platformPrivileges?.manageTokens;

    // Access Tokens list paging.
    const [page, setPage] = useState(1);
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    // Call list Access Token Mutation
    const {
        loading: tokensLoading,
        error: tokensError,
        data: tokensData,
        refetch: tokensRefetch,
    } = useListAccessTokensQuery({
        skip: !canGeneratePersonalAccessTokens || !filters,
        variables: {
            input: {
                start,
                count: pageSize,
                filters,
            },
        },
    });

    const { data: usersData } = useListUsersQuery({
        skip: !canGeneratePersonalAccessTokens || !canManageToken,
        variables: {
            input: {
                start,
                count: 10,
                query: (query?.length && query) || undefined,
            },
        },
        fetchPolicy: 'no-cache',
    });

    useEffect(() => {
        const timestamp = Date.now();
        const lessThanStatus = { field: 'expiresAt', values: [`${timestamp}`], condition: 'LESS_THAN' };
        if (canManageToken) {
            const newFilters: any = owner && owner !== 'All' ? [{ field: 'ownerUrn', values: [owner] }] : [];
            if (statusFilter === StatusType.EXPIRED) {
                newFilters.push(lessThanStatus);
            }
            setFilters(newFilters);
        } else if (filters && statusFilter === StatusType.EXPIRED) {
            const currentUserFilters: any = [...filters];
            currentUserFilters.push(lessThanStatus);
            setFilters(currentUserFilters);
        } else if (filters) {
            setFilters(filters.filter((filter) => filter?.field !== 'expiresAt'));
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [canManageToken, owner, statusFilter]);

    const renderSearchResult = (entity: any) => {
        const { editableProperties } = entity;
        const displayNameSearchResult = entityRegistry.getDisplayName(EntityType.CorpUser, entity);
        const avatarUrl = editableProperties?.pictureLink || undefined;
        return (
            <Select.Option value={entity.urn} key={entity.urn}>
                <OwnerLabel name={displayNameSearchResult} avatarUrl={avatarUrl} type={entity.type} />
            </Select.Option>
        );
    };
    const ownerResult = usersData?.listUsers?.users;

    const ownerSearchOptions = ownerResult?.map((result) => {
        return renderSearchResult(result);
    });

    const totalTokens = tokensData?.listAccessTokens?.total || 0;
    const tokens = useMemo(() => tokensData?.listAccessTokens?.tokens || [], [tokensData]);
    const filteredTokens = tokens.filter((token) => !removedTokens.includes(token.id));

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
                    .then(({ errors }) => {
                        if (!errors) {
                            analytics.event({ type: EventType.RevokeAccessTokenEvent });
                        }
                    })
                    .catch((e) => {
                        message.destroy();
                        message.error({ content: `Failed to revoke Token!: \n ${e.message || ''}`, duration: 3 });
                    })
                    .finally(() => {
                        setTimeout(() => {
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
                if (expiresAt === null) return <NeverExpireText>Never</NeverExpireText>;
                const localeTimezone = getLocaleTimezone();
                const formattedExpireAt = new Date(expiresAt);
                return (
                    <span>{`${formattedExpireAt.toLocaleDateString()} at ${formattedExpireAt.toLocaleTimeString()} (${localeTimezone})`}</span>
                );
            },
        },
        {
            title: 'Owner',
            dataIndex: 'ownerUrn',
            key: 'ownerUrn',
            render: (ownerUrn: string) => {
                if (!ownerUrn) return '';
                const displayName = ownerUrn?.replace('urn:li:corpuser:', '');
                const link = `/user/${ownerUrn}/owner of`;
                const ownerName = displayName || '';
                return <a href={link}>{ownerName}</a>;
            },
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <ActionButtonContainer>
                    <Button
                        onClick={() => onRemoveToken(record)}
                        icon={<DeleteOutlined />}
                        danger
                        data-testid="revoke-token-button"
                    >
                        Revoke
                    </Button>
                </ActionButtonContainer>
            ),
        },
    ];

    const filterColumns = canManageToken ? tableColumns : tableColumns.filter((column) => column.key !== 'ownerUrn');

    const onChangePage = (newPage: number) => {
        scrollToTop();
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
                    {/* NOTE: only for SaaS. If this is brought into OSS, we will need to disable the dropdown and have the button onClick open the personal token modal */}
                    <Dropdown
                        disabled={!canGeneratePersonalAccessTokens}
                        placement="bottom"
                        menu={{
                            items: [
                                {
                                    key: 'personal',
                                    className: 'personal-token-dropdown-option',
                                    label: 'Personal Token',
                                    onClick: () => setCreateTokenFor('personal'),
                                },
                                {
                                    key: 'remote-executor',
                                    className: 'remote-executor-dropdown-option',
                                    label: 'Remote Executor',
                                    onClick: () => setCreateTokenFor('remote-executor'),
                                },
                            ],
                        }}
                    >
                        <Button type="text" data-testid="add-token-button" disabled={!canGeneratePersonalAccessTokens}>
                            <PlusOutlined /> Generate new token
                        </Button>
                    </Dropdown>
                </div>
                <SelectContainer>
                    {canGeneratePersonalAccessTokens && canManageToken && (
                        <>
                            <StyledSelectOwner
                                showSearch
                                placeholder="Search for owner"
                                optionFilterProp="children"
                                allowClear
                                filterOption={false}
                                defaultActiveFirstOption={false}
                                onSelect={(ownerData: any) => {
                                    setOwner(ownerData);
                                }}
                                onClear={() => {
                                    setQuery('');
                                    setOwner('All');
                                }}
                                onSearch={(value: string) => {
                                    setQuery(value.trim());
                                }}
                                style={{ width: 200 }}
                            >
                                {ownerSearchOptions}
                            </StyledSelectOwner>
                        </>
                    )}
                    {canGeneratePersonalAccessTokens && (
                        <StyledSelect
                            value={statusFilter}
                            onChange={(selection) => setStatusFilter(selection as StatusType)}
                            style={{ width: 100 }}
                        >
                            <Select.Option value={StatusType.ALL} key="ALL">
                                All
                            </Select.Option>
                            <Select.Option value={StatusType.EXPIRED} key="EXPIRED">
                                Expired
                            </Select.Option>
                        </StyledSelect>
                    )}
                </SelectContainer>
            </TabToolbar>
            <StyledTable
                columns={filterColumns}
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
                visible={!!createTokenFor}
                forRemoteExecutor={createTokenFor === 'remote-executor'}
                onClose={() => setCreateTokenFor(undefined)}
                onCreateToken={() => {
                    // Hack to deal with eventual consistency.
                    setTimeout(() => {
                        tokensRefetch?.();
                    }, 3000);
                }}
            />
        </SourceContainer>
    );
};
