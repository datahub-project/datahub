import { CloudArrowUp } from '@phosphor-icons/react/dist/csr/CloudArrowUp';
import { FunnelSimple } from '@phosphor-icons/react/dist/csr/FunnelSimple';
import { Key } from '@phosphor-icons/react/dist/csr/Key';
import { Lock } from '@phosphor-icons/react/dist/csr/Lock';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Robot } from '@phosphor-icons/react/dist/csr/Robot';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import CreateTokenModal from '@app/settingsV2/CreateTokenModal';
import SelectServiceAccountModal from '@app/settingsV2/SelectServiceAccountModal';
import { scrollToTop } from '@app/shared/searchUtils';
import { getLocaleTimezone } from '@app/shared/time/timeUtils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import {
    Alert,
    Avatar,
    Button,
    EmptyState,
    Menu,
    PageTitle,
    Pagination,
    SimpleSelect,
    Table,
    Text,
    Tooltip,
    toast,
} from '@src/alchemy-components';
import { ItemType } from '@src/alchemy-components/components/Menu/types';
import { Column } from '@src/alchemy-components/components/Table/types';
import { spacing } from '@src/alchemy-components/theme';

import { ListAccessTokensQuery, useListAccessTokensQuery, useRevokeAccessTokenMutation } from '@graphql/auth.generated';
import { useListUsersQuery } from '@graphql/user.generated';
import { AccessTokenType, EntityType, FacetFilterInput, FilterOperator, ServiceAccount } from '@types';

const SourceContainer = styled.div`
    width: 100%;
    height: 100%;
    padding: ${spacing.md} ${spacing.lg};
    display: flex;
    flex-direction: column;
    gap: ${spacing.md};
    overflow: hidden;
`;

const PageHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const NeverExpireText = styled.span`
    color: ${(props) => props.theme.colors.textError};
`;

const TableContainer = styled.div`
    flex: 1;
    min-height: 0;
    overflow: auto;

    table {
        table-layout: fixed;
    }
`;

const TruncatedText = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    display: block;
`;

const DescriptionText = styled(Text)`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    display: block;
`;

const SelectContainer = styled.div`
    display: flex;
    align-items: flex-start;
    gap: ${spacing.xsm};
`;

const DEFAULT_PAGE_SIZE = 10;

const ACTION_CELL_STYLE: React.CSSProperties = { display: 'flex', justifyContent: 'flex-end' };

type TokenMetadata = ListAccessTokensQuery['listAccessTokens']['tokens'][number];

type TokenRow = {
    urn: string;
    id: string;
    name: string;
    description: string | null | undefined;
    actorUrn: string;
    ownerUrn: string;
    owner: TokenMetadata['owner'];
    createdAt: number;
    expiresAt: number | null | undefined;
};

interface OwnerOption {
    value: string;
    label: string;
    imageUrl: string | undefined;
}

enum StatusType {
    ALL,
    EXPIRED,
}

export const AccessTokens = () => {
    const { t } = useTranslation('settings.tokens');
    const [createTokenFor, setCreateTokenFor] = useState<'personal' | 'remote-executor' | undefined>(undefined);
    const [showSelectServiceAccountModal, setShowSelectServiceAccountModal] = useState(false);
    const [selectedServiceAccount, setSelectedServiceAccount] = useState<ServiceAccount | null>(null);
    const [removedTokens, setRemovedTokens] = useState<string[]>([]);
    const [statusFilter, setStatusFilter] = useState(StatusType.ALL);
    const [owner, setOwner] = useState<string | undefined>(undefined);
    const [filters, setFilters] = useState<Array<FacetFilterInput> | null>(null);
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [tokenToBeRemoved, setTokenToBeRemoved] = useState<TokenRow | null>(null);
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
    const canManageServiceAccounts = authenticatedUser?.platformPrivileges?.manageServiceAccounts;

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
        const lessThanStatus: FacetFilterInput = {
            field: 'expiresAt',
            values: [`${timestamp}`],
            condition: FilterOperator.LessThan,
        };
        if (canManageToken) {
            const newFilters: FacetFilterInput[] = owner ? [{ field: 'ownerUrn', values: [owner] }] : [];
            if (statusFilter === StatusType.EXPIRED) {
                newFilters.push(lessThanStatus);
            }
            setFilters(newFilters);
        } else if (filters && statusFilter === StatusType.EXPIRED) {
            const currentUserFilters: FacetFilterInput[] = [...filters];
            currentUserFilters.push(lessThanStatus);
            setFilters(currentUserFilters);
        } else if (filters) {
            setFilters(filters.filter((filter) => filter?.field !== 'expiresAt'));
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [canManageToken, owner, statusFilter]);

    const ownerResult = usersData?.listUsers?.users;

    const ownerOptions: OwnerOption[] = useMemo(
        () =>
            ownerResult?.map((entity) => ({
                value: entity.urn,
                label: entityRegistry.getDisplayName(EntityType.CorpUser, entity),
                imageUrl: entity.editableProperties?.pictureLink || undefined,
            })) || [],
        [ownerResult, entityRegistry],
    );

    const totalTokens = tokensData?.listAccessTokens?.total || 0;
    const tokens = useMemo(() => tokensData?.listAccessTokens?.tokens || [], [tokensData]);
    const filteredTokens = tokens.filter((token) => !removedTokens.includes(token.id));

    const [revokeAccessToken, { error: revokeTokenError }] = useRevokeAccessTokenMutation();

    // Revoke token Handler
    const onRemoveToken = (token: TokenRow) => {
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
                toast.destroy();
                toast.error(t('revokeError', { message: e.message || '' }));
            })
            .finally(() => {
                setTimeout(() => {
                    tokensRefetch?.();
                }, 3000);
            });
    };

    const tableData: TokenRow[] = filteredTokens?.map((token) => ({
        urn: token.urn,
        id: token.id,
        name: token.name,
        description: token.description,
        actorUrn: token.actorUrn,
        ownerUrn: token.ownerUrn,
        owner: token.owner,
        createdAt: token.createdAt,
        expiresAt: token.expiresAt,
    }));

    const tableColumns: Column<TokenRow>[] = [
        {
            title: t('columnName'),
            key: 'name',
            width: '27%',
            render: (record) => (
                <Tooltip title={record.name} showArrow={false}>
                    <TruncatedText>
                        <b>{record.name}</b>
                    </TruncatedText>
                </Tooltip>
            ),
        },
        {
            title: t('columnDescription'),
            key: 'description',
            width: '25%',
            render: (record) =>
                record.description ? (
                    <Tooltip title={record.description} showArrow={false}>
                        <DescriptionText size="md">{record.description}</DescriptionText>
                    </Tooltip>
                ) : null,
        },
        {
            title: t('columnExpiresAt'),
            key: 'expiresAt',
            width: '25%',
            render: (record) => {
                if (!record.expiresAt) return <NeverExpireText>{t('neverExpires')}</NeverExpireText>;
                const localeTimezone = getLocaleTimezone();
                const formattedExpireAt = new Date(record.expiresAt);
                return (
                    <span>
                        {t('expiresAtFormat', {
                            date: formattedExpireAt.toLocaleDateString(),
                            time: formattedExpireAt.toLocaleTimeString(),
                            timezone: localeTimezone,
                        })}
                    </span>
                );
            },
        },
        {
            title: t('columnOwner'),
            key: 'owner',
            width: '15%',
            render: (record) => {
                if (!record.owner && !record.ownerUrn) return null;
                const ownerUrn = record.owner?.urn || record.ownerUrn;
                const displayName = record.owner
                    ? entityRegistry.getDisplayName(EntityType.CorpUser, record.owner)
                    : ownerUrn?.replace('urn:li:corpuser:', '');
                const avatarUrl = record.owner?.editableProperties?.pictureLink || undefined;
                return (
                    <Avatar
                        name={displayName || ''}
                        imageUrl={avatarUrl}
                        size="sm"
                        showInPill
                        onClick={() => {
                            const link = `/${entityRegistry.getPathName(EntityType.CorpUser)}/${encodeURIComponent(ownerUrn)}`;
                            window.location.href = link;
                        }}
                    />
                );
            },
        },
        {
            title: '',
            key: 'actions',
            width: '8%',
            alignment: 'right',
            render: (record) => (
                <div style={ACTION_CELL_STYLE}>
                    <Button
                        onClick={() => setTokenToBeRemoved(record)}
                        icon={{ icon: Trash, size: 'xl' }}
                        variant="text"
                        color="red"
                        isCircle
                        size="lg"
                        aria-label={t('revokeTokenAriaLabel')}
                        data-testid="revoke-token-button"
                    />
                </div>
            ),
        },
    ];

    const filterColumns = canManageToken ? tableColumns : tableColumns.filter((column) => column.key !== 'owner');

    const hasActiveFilters = (canManageToken && !!owner) || statusFilter !== StatusType.ALL;

    const clearFilters = () => {
        setOwner(undefined);
        setStatusFilter(StatusType.ALL);
    };

    const renderTokensContent = () => {
        if (tokensLoading || (canGeneratePersonalAccessTokens && filters && !tokensData)) {
            return <Table columns={filterColumns} data={[]} rowKey="urn" showHeader isLoading isScrollable />;
        }
        if (tableData.length > 0) {
            return <Table columns={filterColumns} data={tableData} rowKey="urn" showHeader isScrollable />;
        }
        if (!canGeneratePersonalAccessTokens) {
            return (
                <EmptyState
                    icon={Lock}
                    title={t('noAccessTitle')}
                    description={t('noAccessDescription')}
                    style={{ flex: 1 }}
                />
            );
        }
        if (hasActiveFilters) {
            return (
                <EmptyState
                    icon={FunnelSimple}
                    title={t('noTokensFoundTitle')}
                    description={t('noTokensFoundDescription')}
                    action={{
                        label: t('clearFilters'),
                        onClick: clearFilters,
                        icon: { icon: X },
                        variant: 'secondary',
                    }}
                    style={{ flex: 1 }}
                />
            );
        }
        return (
            <EmptyState
                icon={Key}
                title={t('emptyTitle')}
                description={t('emptyDescription')}
                action={{
                    label: t('generateNewToken'),
                    onClick: () => setCreateTokenFor('personal'),
                    icon: { icon: Plus },
                }}
                style={{ flex: 1 }}
            />
        );
    };

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    return (
        <SourceContainer data-testid="manage-access-tokens-page">
            {tokensError && <Alert variant="error" title={t('loadError')} />}
            {revokeTokenError && <Alert variant="error" title={t('updateError')} />}
            <PageHeader>
                <PageTitle title={t('pageTitle')} subTitle={t('pageSubTitle')} />
                <Menu
                    disabled={!canGeneratePersonalAccessTokens}
                    placement="bottom"
                    items={[
                        {
                            type: 'item',
                            key: 'personal',
                            title: t('personalToken'),
                            icon: Key,
                            onClick: () => setCreateTokenFor('personal'),
                        },
                        {
                            type: 'item',
                            key: 'remote-executor',
                            title: t('remoteExecutor'),
                            icon: CloudArrowUp,
                            onClick: () => setCreateTokenFor('remote-executor'),
                        },
                        ...(canManageServiceAccounts
                            ? ([
                                  {
                                      type: 'item',
                                      key: 'service-account',
                                      title: t('serviceAccount'),
                                      icon: Robot,
                                      onClick: () => setShowSelectServiceAccountModal(true),
                                  },
                              ] as ItemType[])
                            : []),
                    ]}
                >
                    <Button
                        variant="filled"
                        icon={{ icon: Plus }}
                        data-testid="add-token-button"
                        disabled={!canGeneratePersonalAccessTokens}
                    >
                        {t('generateNewToken')}
                    </Button>
                </Menu>
            </PageHeader>
            {isTokenAuthEnabled === false && <Alert variant="error" title={t('authDisabledAlert')} />}
            <SelectContainer>
                {canGeneratePersonalAccessTokens && canManageToken && (
                    <SimpleSelect
                        options={ownerOptions}
                        values={owner ? [owner] : []}
                        onUpdate={(values) => setOwner(values.length > 0 ? values[0] : undefined)}
                        onClear={() => {
                            setQuery('');
                            setOwner(undefined);
                        }}
                        showSearch
                        filterResultsByQuery={false}
                        onSearchChange={(value) => setQuery(value.trim())}
                        placeholder={t('ownerPlaceholder')}
                        showClear
                        width="fit-content"
                        renderCustomOptionText={(option) => {
                            const ownerOpt = ownerOptions.find((o) => o.value === option.value);
                            return (
                                <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                    <Avatar name={option.label} imageUrl={ownerOpt?.imageUrl} size="sm" />
                                    <span>{option.label}</span>
                                </span>
                            );
                        }}
                    />
                )}
                {canGeneratePersonalAccessTokens && (
                    <SimpleSelect
                        options={[
                            { value: String(StatusType.ALL), label: t('statusAll') },
                            { value: String(StatusType.EXPIRED), label: t('statusExpired') },
                        ]}
                        values={[String(statusFilter)]}
                        onUpdate={(values) => setStatusFilter(Number(values[0]) as StatusType)}
                        showClear={false}
                        width="fit-content"
                    />
                )}
            </SelectContainer>
            <TableContainer>{renderTokensContent()}</TableContainer>
            {totalTokens > 0 && (
                <Pagination
                    currentPage={page}
                    itemsPerPage={pageSize}
                    total={totalTokens}
                    onPageChange={onChangePage}
                    showSizeChanger={false}
                />
            )}
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
            <ConfirmationModal
                isOpen={!!tokenToBeRemoved}
                handleClose={() => setTokenToBeRemoved(null)}
                handleConfirm={() => {
                    if (tokenToBeRemoved) onRemoveToken(tokenToBeRemoved);
                    setTokenToBeRemoved(null);
                }}
                modalTitle={t('revokeConfirmTitle')}
                modalText={t('revokeConfirmText')}
            />
            <SelectServiceAccountModal
                visible={showSelectServiceAccountModal}
                onClose={() => setShowSelectServiceAccountModal(false)}
                onSelectServiceAccount={(serviceAccount) => {
                    setShowSelectServiceAccountModal(false);
                    setSelectedServiceAccount(serviceAccount);
                }}
            />
            {selectedServiceAccount && (
                <CreateTokenModal
                    visible={!!selectedServiceAccount}
                    actorUrn={selectedServiceAccount.urn}
                    tokenType={AccessTokenType.ServiceAccount}
                    actorDisplayName={selectedServiceAccount.displayName || selectedServiceAccount.name}
                    onClose={() => setSelectedServiceAccount(null)}
                    onCreateToken={() => {
                        setSelectedServiceAccount(null);
                        tokensRefetch?.();
                    }}
                />
            )}
        </SourceContainer>
    );
};
