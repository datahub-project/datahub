import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { Key } from '@phosphor-icons/react/dist/csr/Key';
import { Robot } from '@phosphor-icons/react/dist/csr/Robot';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import { NO_ROLE_URN } from '@app/identity/useRoleAssignment';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import CreateTokenModal from '@app/settingsV2/CreateTokenModal';
import {
    Avatar,
    Button,
    Icon,
    Modal,
    Pagination,
    SearchBar,
    SimpleSelect,
    Table,
    Text,
    Tooltip,
} from '@src/alchemy-components';
import { Menu } from '@src/alchemy-components/components/Menu';
import { ItemType } from '@src/alchemy-components/components/Menu/types';
import { SelectOption } from '@src/alchemy-components/components/Select/types';

import { AccessTokenType, DataHubRole, ServiceAccount } from '@types';

const ServiceAccountContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 16px;
`;

const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: hidden;

    table {
        table-layout: fixed;
        width: 100%;
    }
`;

const FiltersHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
`;

const SearchContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

const MinCharsHint = styled(Text)`
    margin-top: 4px;
`;

const ActionsContainer = styled.div`
    display: flex;
    align-items: right;
    justify-content: flex-end;
    gap: 12px;
`;

export const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

const ServiceAccountInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
    overflow: hidden;
`;

const ServiceAccountDetails = styled.div`
    display: flex;
    flex-direction: column;
    color: ${(props) => props.theme.colors.textSecondary};
    overflow: hidden;
    min-width: 0;
`;

const ActionsButtonStyle = {
    background: 'none',
    border: 'none',
    boxShadow: 'none',
};

const EllipsisText = styled(Text)`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 100%;
    display: block;
`;

export const EmptyStateContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 60px 20px;
    text-align: center;
    gap: 16px;
`;

type ServiceAccountNameCellProps = {
    serviceAccount: ServiceAccount;
};

const ServiceAccountNameCell = ({ serviceAccount }: ServiceAccountNameCellProps) => {
    const displayName = serviceAccount.displayName || serviceAccount.name;

    return (
        <ServiceAccountInfo>
            <Avatar size="xl" name={displayName} />
            <ServiceAccountDetails>
                <Tooltip title={displayName} showArrow={false}>
                    <EllipsisText size="md" weight="semiBold" lineHeight="xs">
                        {displayName}
                    </EllipsisText>
                </Tooltip>
            </ServiceAccountDetails>
        </ServiceAccountInfo>
    );
};

type ServiceAccountDescriptionCellProps = {
    serviceAccount: ServiceAccount;
};

export const ServiceAccountDescriptionCell = ({ serviceAccount }: ServiceAccountDescriptionCellProps) => {
    const { t } = useTranslation('entity.identity');

    if (!serviceAccount.description) {
        return (
            <Text color="gray" size="md">
                {t('serviceAccounts.descriptionEmpty')}
            </Text>
        );
    }

    return (
        <Tooltip title={serviceAccount.description} showArrow={false}>
            <EllipsisText color="gray" size="md">
                {serviceAccount.description}
            </EllipsisText>
        </Tooltip>
    );
};

type ServiceAccountRoleCellProps = {
    serviceAccount: ServiceAccount;
    selectRoleOptions: DataHubRole[];
    optimisticRoleUrn?: string;
    onRoleChange?: (serviceAccountUrn: string, newRoleUrn: string, originalRoleUrn: string) => void;
};

const ServiceAccountRoleCell = ({
    serviceAccount,
    selectRoleOptions,
    optimisticRoleUrn,
    onRoleChange,
}: ServiceAccountRoleCellProps) => {
    const { t } = useTranslation('entity.identity');
    // TODO: Remove cast once GraphQL codegen includes roles on ServiceAccount
    const castedServiceAccount = serviceAccount as any;
    const roleRelationships = castedServiceAccount?.roles?.relationships;
    const serverRole =
        roleRelationships && roleRelationships.length > 0 && (roleRelationships[0]?.entity as DataHubRole);
    const serverRoleUrn = serverRole?.urn || 'urn:li:dataHubRole:NoRole';

    // Use optimistic role if available, otherwise use server data
    const currentRoleUrn = optimisticRoleUrn ?? serverRoleUrn;

    return (
        <SimpleSelectRole
            selectedRole={selectRoleOptions.find((r) => r.urn === currentRoleUrn)}
            onRoleSelect={(role) => {
                const newRoleUrn = role?.urn || NO_ROLE_URN;
                if (newRoleUrn !== currentRoleUrn && onRoleChange) {
                    onRoleChange(serviceAccount.urn, newRoleUrn, serverRoleUrn);
                }
            }}
            placeholder={t('serviceAccounts.noRole')}
            size="md"
            width="fit-content"
        />
    );
};

type ServiceAccountDefaultViewCellProps = {
    serviceAccount: ServiceAccount;
    viewOptions: SelectOption[];
    onDefaultViewChange: (serviceAccountUrn: string, viewUrn: string | null) => void;
};

export const ServiceAccountDefaultViewCell = ({
    serviceAccount,
    viewOptions,
    onDefaultViewChange,
}: ServiceAccountDefaultViewCellProps) => {
    const { t } = useTranslation('entity.identity');
    const currentViewUrn = serviceAccount.defaultView?.urn;
    const resolvedValue = viewOptions.some((o) => o.value === currentViewUrn) ? currentViewUrn : undefined;

    return (
        <Tooltip title={t('serviceAccounts.defaultViewTooltip')} showArrow={false}>
            <div>
                <SimpleSelect
                    options={viewOptions}
                    values={resolvedValue ? [resolvedValue] : []}
                    placeholder={t('serviceAccounts.noView')}
                    onUpdate={(values) => {
                        const nextView = values[0] ?? null;
                        if (nextView !== currentViewUrn) {
                            onDefaultViewChange(serviceAccount.urn, nextView);
                        }
                    }}
                    onClear={() => onDefaultViewChange(serviceAccount.urn, null)}
                    size="md"
                    width="fit-content"
                    showClear={!!resolvedValue}
                    showSearch
                />
            </div>
        </Tooltip>
    );
};

type ServiceAccountActionsMenuProps = {
    serviceAccount: ServiceAccount;
    onDelete: (urn: string) => void;
};

const ServiceAccountActionsMenu = ({ serviceAccount, onDelete }: ServiceAccountActionsMenuProps) => {
    const { t } = useTranslation('entity.identity');
    const { t: tc } = useTranslation('common.actions');
    const history = useHistory();
    const [isCreatingToken, setIsCreatingToken] = useState(false);
    const [isConfirmingDelete, setIsConfirmingDelete] = useState(false);

    const displayName = serviceAccount.displayName || serviceAccount.name;

    const handleCopyUrn = () => {
        navigator.clipboard.writeText(serviceAccount.urn);
        message.success(t('serviceAccounts.urnCopied'));
    };

    const handleTokenCreated = () => {
        setIsCreatingToken(false);
        history.push('/settings/tokens');
    };

    const handleDeleteConfirm = () => {
        onDelete(serviceAccount.urn);
        setIsConfirmingDelete(false);
    };

    const items: ItemType[] = [
        {
            type: 'item' as const,
            key: 'create-token',
            title: t('serviceAccounts.createToken'),
            icon: Key,
            onClick: () => setIsCreatingToken(true),
        },
        {
            type: 'item' as const,
            key: 'copy-urn',
            title: t('serviceAccounts.copyUrn'),
            icon: Copy,
            onClick: handleCopyUrn,
        },
        {
            type: 'item' as const,
            key: 'delete',
            title: tc('delete'),
            icon: Trash,
            danger: true,
            onClick: () => setIsConfirmingDelete(true),
        },
    ];

    return (
        <>
            <Menu items={items}>
                <Button
                    variant="text"
                    icon={{ icon: DotsThreeVertical, weight: 'bold', size: 'xl', color: 'gray' }}
                    isCircle
                    style={ActionsButtonStyle}
                    data-testid={`service-account-menu-${serviceAccount.name}`}
                />
            </Menu>
            {isCreatingToken && (
                <CreateTokenModal
                    visible={isCreatingToken}
                    actorUrn={serviceAccount.urn}
                    tokenType={AccessTokenType.ServiceAccount}
                    actorDisplayName={displayName}
                    onClose={() => setIsCreatingToken(false)}
                    onCreateToken={handleTokenCreated}
                />
            )}
            {isConfirmingDelete && (
                <Modal
                    open={isConfirmingDelete}
                    title={t('serviceAccounts.deleteTitle')}
                    onCancel={() => setIsConfirmingDelete(false)}
                    dataTestId="delete-service-account-modal"
                    footer={
                        <ModalFooter>
                            <Button
                                variant="outline"
                                onClick={() => setIsConfirmingDelete(false)}
                                data-testid="delete-service-account-cancel-button"
                            >
                                {tc('cancel')}
                            </Button>
                            <Button
                                variant="filled"
                                color="red"
                                onClick={handleDeleteConfirm}
                                data-testid="delete-service-account-confirm-button"
                            >
                                {tc('delete')}
                            </Button>
                        </ModalFooter>
                    }
                >
                    <Text>{t('serviceAccounts.deleteConfirm', { name: displayName })}</Text>
                </Modal>
            )}
        </>
    );
};

type ServiceAccountTableProps = {
    query: string;
    setQuery: (query: string) => void;
    setPage: (page: number) => void;
    serviceAccounts: ServiceAccount[];
    selectRoleOptions: DataHubRole[];
    optimisticRoles: Record<string, string>;
    viewOptions: SelectOption[];
    loading: boolean;
    page: number;
    pageSize: number;
    total: number;
    onChangePage: (page: number) => void;
    onDelete: (urn: string) => void;
    onRoleChange?: (serviceAccountUrn: string, newRoleUrn: string, originalRoleUrn: string) => void;
    onDefaultViewChange: (serviceAccountUrn: string, viewUrn: string | null) => void;
    refetch?: () => void;
};

export const ServiceAccountTable = ({
    query,
    setQuery,
    setPage,
    serviceAccounts,
    selectRoleOptions,
    optimisticRoles,
    viewOptions,
    loading,
    page,
    pageSize,
    total,
    onChangePage,
    onDelete,
    onRoleChange,
    onDefaultViewChange,
    refetch: _refetch,
}: ServiceAccountTableProps) => {
    const { t } = useTranslation('entity.identity');

    const columns = [
        {
            title: t('serviceAccounts.table.name'),
            dataIndex: 'name',
            key: 'name',
            width: '25%',
            render: (serviceAccount: ServiceAccount) => <ServiceAccountNameCell serviceAccount={serviceAccount} />,
        },
        {
            title: t('serviceAccounts.table.description'),
            dataIndex: 'description',
            key: 'description',
            width: '30%',
            render: (serviceAccount: ServiceAccount) => (
                <ServiceAccountDescriptionCell serviceAccount={serviceAccount} />
            ),
        },
        {
            title: t('serviceAccounts.table.role'),
            key: 'role',
            width: '15%',
            render: (serviceAccount: ServiceAccount) => (
                <ServiceAccountRoleCell
                    serviceAccount={serviceAccount}
                    selectRoleOptions={selectRoleOptions}
                    optimisticRoleUrn={optimisticRoles[serviceAccount.urn]}
                    onRoleChange={onRoleChange}
                />
            ),
        },
        {
            title: (
                <Tooltip title={t('serviceAccounts.defaultViewTooltip')} showArrow={false}>
                    <span>{t('serviceAccounts.table.defaultView')}</span>
                </Tooltip>
            ),
            key: 'defaultView',
            width: '20%',
            render: (serviceAccount: ServiceAccount) => (
                <ServiceAccountDefaultViewCell
                    serviceAccount={serviceAccount}
                    viewOptions={viewOptions}
                    onDefaultViewChange={onDefaultViewChange}
                />
            ),
        },
        {
            title: '',
            key: 'actions',
            width: '10%',
            render: (serviceAccount: ServiceAccount) => (
                <ActionsContainer>
                    <ServiceAccountActionsMenu serviceAccount={serviceAccount} onDelete={onDelete} />
                </ActionsContainer>
            ),
        },
    ];

    return (
        <>
            <ServiceAccountContainer>
                <FiltersHeader>
                    <SearchContainer>
                        <SearchBar
                            placeholder={t('serviceAccounts.searchPlaceholder')}
                            value={query}
                            onChange={(value) => {
                                setQuery(value);
                                setPage(1);
                            }}
                            width="300px"
                            allowClear
                        />
                        {query.length > 0 && query.length < 3 && (
                            <MinCharsHint size="xs" color="gray">
                                {t('serviceAccounts.searchMinChars')}
                            </MinCharsHint>
                        )}
                    </SearchContainer>
                </FiltersHeader>
            </ServiceAccountContainer>

            <TableContainer data-testid="service-accounts-table-container">
                {serviceAccounts.length > 0 ? (
                    <>
                        <Table columns={columns} data={serviceAccounts} isLoading={loading} isScrollable />
                        <div style={{ paddingTop: '8px', display: 'flex', justifyContent: 'center' }}>
                            <Pagination
                                currentPage={page}
                                itemsPerPage={pageSize}
                                total={total}
                                onPageChange={onChangePage}
                            />
                        </div>
                    </>
                ) : (
                    <EmptyStateContainer>
                        {loading ? (
                            <Text size="md" color="gray">
                                {t('serviceAccounts.loading')}
                            </Text>
                        ) : (
                            <>
                                <Icon icon={Robot} size="4xl" color="gray" />
                                <Text size="md" color="gray">
                                    {t('serviceAccounts.emptyTitle')}
                                </Text>
                                <Text size="sm" color="gray">
                                    {t('serviceAccounts.emptyDescription')}
                                </Text>
                            </>
                        )}
                    </EmptyStateContainer>
                )}
            </TableContainer>
        </>
    );
};
