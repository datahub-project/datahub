import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { Key } from '@phosphor-icons/react/dist/csr/Key';
import { Robot } from '@phosphor-icons/react/dist/csr/Robot';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { message } from 'antd';
import React, { useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

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
    if (!serviceAccount.description) {
        return (
            <Text color="gray" size="md">
                -
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
                const newRoleUrn = role?.urn || 'urn:li:dataHubRole:NoRole';
                if (newRoleUrn !== currentRoleUrn && onRoleChange) {
                    onRoleChange(serviceAccount.urn, newRoleUrn, serverRoleUrn);
                }
            }}
            placeholder="No Role"
            size="md"
            width="fit-content"
        />
    );
};

const DEFAULT_VIEW_TOOLTIP =
    'Set a default view for the service account. If a view is selected, the account will focus on ' +
    'browsing assets within the view when accessing DataHub using an AI agent (MCP server). ' +
    'Only public (global) views are supported.';

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
    const currentViewUrn = serviceAccount.defaultView?.urn;
    const resolvedValue = viewOptions.some((o) => o.value === currentViewUrn) ? currentViewUrn : undefined;

    return (
        <Tooltip title={DEFAULT_VIEW_TOOLTIP} showArrow={false}>
            <div>
                <SimpleSelect
                    options={viewOptions}
                    values={resolvedValue ? [resolvedValue] : []}
                    placeholder="No view"
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
    const history = useHistory();
    const [isCreatingToken, setIsCreatingToken] = useState(false);
    const [isConfirmingDelete, setIsConfirmingDelete] = useState(false);

    const displayName = serviceAccount.displayName || serviceAccount.name;

    const handleCopyUrn = () => {
        navigator.clipboard.writeText(serviceAccount.urn);
        message.success('URN copied to clipboard');
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
            title: 'Create Token',
            icon: Key,
            onClick: () => setIsCreatingToken(true),
        },
        {
            type: 'item' as const,
            key: 'copy-urn',
            title: 'Copy URN',
            icon: Copy,
            onClick: handleCopyUrn,
        },
        {
            type: 'item' as const,
            key: 'delete',
            title: 'Delete',
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
                    title="Delete Service Account"
                    onCancel={() => setIsConfirmingDelete(false)}
                    dataTestId="delete-service-account-modal"
                    footer={
                        <ModalFooter>
                            <Button
                                variant="outline"
                                onClick={() => setIsConfirmingDelete(false)}
                                data-testid="delete-service-account-cancel-button"
                            >
                                Cancel
                            </Button>
                            <Button
                                variant="filled"
                                color="red"
                                onClick={handleDeleteConfirm}
                                data-testid="delete-service-account-confirm-button"
                            >
                                Delete
                            </Button>
                        </ModalFooter>
                    }
                >
                    <Text>
                        Are you sure you want to delete the service account &quot;{displayName}&quot;? This action
                        cannot be undone and will revoke all associated API tokens.
                    </Text>
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
    const columns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            width: '25%',
            render: (serviceAccount: ServiceAccount) => <ServiceAccountNameCell serviceAccount={serviceAccount} />,
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            width: '30%',
            render: (serviceAccount: ServiceAccount) => (
                <ServiceAccountDescriptionCell serviceAccount={serviceAccount} />
            ),
        },
        {
            title: 'Role',
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
                <Tooltip title={DEFAULT_VIEW_TOOLTIP} showArrow={false}>
                    <span>Default View</span>
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
                            placeholder="Search service accounts..."
                            value={query}
                            onChange={(value) => {
                                setQuery(value);
                                setPage(1);
                            }}
                            width="300px"
                            allowClear
                        />
                        {query.length > 0 && query.length < 3 && (
                            <Text size="xs" color="gray" style={{ marginTop: '4px' }}>
                                Enter at least 3 characters to search
                            </Text>
                        )}
                    </SearchContainer>
                </FiltersHeader>
            </ServiceAccountContainer>

            <TableContainer>
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
                                Loading service accounts...
                            </Text>
                        ) : (
                            <>
                                <Icon icon={Robot} size="4xl" color="gray" />
                                <Text size="md" color="gray">
                                    No service accounts found
                                </Text>
                                <Text size="sm" color="gray">
                                    Create a service account to enable programmatic access to DataHub APIs
                                </Text>
                            </>
                        )}
                    </EmptyStateContainer>
                )}
            </TableContainer>
        </>
    );
};
