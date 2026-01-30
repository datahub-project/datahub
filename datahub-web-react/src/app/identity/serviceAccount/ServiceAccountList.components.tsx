import { message } from 'antd';
import React, { useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import CreateTokenModal from '@app/settingsV2/CreateTokenModal';
import { Avatar, Button, Icon, Modal, Pagination, SearchBar, Table, Text, colors } from '@src/alchemy-components';
import { Menu } from '@src/alchemy-components/components/Menu';
import { ItemType } from '@src/alchemy-components/components/Menu/types';

import { AccessTokenType, DataHubRole, ServiceAccount } from '@types';

export const ServiceAccountContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 16px;
    padding: 0 16px;
`;

export const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    max-height: calc(100vh - 330px);
    overflow: auto;
    padding: 0 16px;

    /* Make table header sticky */
    .ant-table-thead {
        position: sticky;
        top: 0;
        z-index: 1;
        background: white;
    }

    /* Ensure header cells have proper background */
    .ant-table-thead > tr > th {
        background: white !important;
        border-bottom: 1px solid #f0f0f0;
    }
`;

export const FiltersHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
`;

export const SearchContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

export const ActionsContainer = styled.div`
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
`;

const ServiceAccountDetails = styled.div`
    display: flex;
    flex-direction: column;
    color: ${colors.gray[600]};
`;

const ActionsButtonStyle = {
    background: 'none',
    border: 'none',
    boxShadow: 'none',
};

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

export const ServiceAccountNameCell = ({ serviceAccount }: ServiceAccountNameCellProps) => {
    const displayName = serviceAccount.displayName || serviceAccount.name;

    return (
        <ServiceAccountInfo>
            <Avatar size="xl" name={displayName} />
            <ServiceAccountDetails>
                <Text size="md" weight="semiBold" lineHeight="xs">
                    {displayName}
                </Text>
            </ServiceAccountDetails>
        </ServiceAccountInfo>
    );
};

type ServiceAccountDescriptionCellProps = {
    serviceAccount: ServiceAccount;
};

export const ServiceAccountDescriptionCell = ({ serviceAccount }: ServiceAccountDescriptionCellProps) => {
    return (
        <Text color="gray" size="md">
            {serviceAccount.description || '-'}
        </Text>
    );
};

type ServiceAccountRoleCellProps = {
    serviceAccount: ServiceAccount;
    selectRoleOptions: DataHubRole[];
    optimisticRoleUrn?: string;
    onRoleChange?: (serviceAccountUrn: string, newRoleUrn: string, originalRoleUrn: string) => void;
};

export const ServiceAccountRoleCell = ({
    serviceAccount,
    selectRoleOptions,
    optimisticRoleUrn,
    onRoleChange,
}: ServiceAccountRoleCellProps) => {
    // Extract current role from relationships (server data)
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

type ServiceAccountActionsMenuProps = {
    serviceAccount: ServiceAccount;
    onDelete: (urn: string) => void;
    refetch?: () => void;
};

export const ServiceAccountActionsMenu = ({
    serviceAccount,
    onDelete,
    refetch: _refetch,
}: ServiceAccountActionsMenuProps) => {
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
            icon: 'Key',
            onClick: () => setIsCreatingToken(true),
        },
        {
            type: 'item' as const,
            key: 'copy-urn',
            title: 'Copy URN',
            icon: 'Copy',
            onClick: handleCopyUrn,
        },
        {
            type: 'item' as const,
            key: 'delete',
            title: 'Delete',
            icon: 'Trash',
            danger: true,
            onClick: () => setIsConfirmingDelete(true),
        },
    ];

    return (
        <>
            <Menu items={items}>
                <Button
                    variant="text"
                    icon={{ icon: 'DotsThreeVertical', weight: 'bold', size: 'xl', source: 'phosphor', color: 'gray' }}
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
    loading: boolean;
    page: number;
    pageSize: number;
    total: number;
    onChangePage: (page: number) => void;
    onDelete: (urn: string) => void;
    onRoleChange?: (serviceAccountUrn: string, newRoleUrn: string, originalRoleUrn: string) => void;
    refetch?: () => void;
};

export const ServiceAccountTable = ({
    query,
    setQuery,
    setPage,
    serviceAccounts,
    selectRoleOptions,
    optimisticRoles,
    loading,
    page,
    pageSize,
    total,
    onChangePage,
    onDelete,
    onRoleChange,
    refetch,
}: ServiceAccountTableProps) => {
    const columns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            minWidth: '30%',
            render: (serviceAccount: ServiceAccount) => <ServiceAccountNameCell serviceAccount={serviceAccount} />,
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            minWidth: '35%',
            render: (serviceAccount: ServiceAccount) => (
                <ServiceAccountDescriptionCell serviceAccount={serviceAccount} />
            ),
        },
        {
            title: 'Role',
            key: 'role',
            minWidth: '15%',
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
            title: '',
            key: 'actions',
            minWidth: '5%',
            render: (serviceAccount: ServiceAccount) => (
                <ActionsContainer>
                    <ServiceAccountActionsMenu serviceAccount={serviceAccount} onDelete={onDelete} refetch={refetch} />
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
                        <div style={{ padding: '8px 20px 0 20px', display: 'flex', justifyContent: 'center' }}>
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
                                <Icon icon="Robot" source="phosphor" size="4xl" color="gray" />
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
