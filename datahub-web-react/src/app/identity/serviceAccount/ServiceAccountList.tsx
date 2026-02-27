import React from 'react';
import styled from 'styled-components/macro';

import CreateServiceAccountModal from '@app/identity/serviceAccount/CreateServiceAccountModal';
import { ModalFooter, ServiceAccountTable } from '@app/identity/serviceAccount/ServiceAccountList.components';
import {
    useServiceAccountListData,
    useServiceAccountListState,
    useServiceAccountRoleAssignment,
} from '@app/identity/serviceAccount/ServiceAccountList.hooks';
import { Message } from '@app/shared/Message';
import { Button, Icon, Modal, Text } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

const PageContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: auto;
`;

const NoPermissionContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 60px 20px;
    text-align: center;
    gap: 16px;
    color: ${colors.gray[600]};
`;

type ServiceAccountListProps = {
    /** When true, the Create button is controlled externally (e.g., in page header) */
    isCreatingServiceAccount?: boolean;
    setIsCreatingServiceAccount?: (value: boolean) => void;
};

export const ServiceAccountList = ({
    isCreatingServiceAccount: externalIsCreating,
    setIsCreatingServiceAccount: externalSetIsCreating,
}: ServiceAccountListProps) => {
    const {
        query,
        setQuery,
        debouncedQuery,
        page,
        setPage,
        pageSize,
        isCreatingServiceAccount: internalIsCreating,
        setIsCreatingServiceAccount: internalSetIsCreating,
        roleAssignmentState,
        setRoleAssignmentState,
        optimisticRoles,
        setOptimisticRoles,
    } = useServiceAccountListState();

    // If external state is provided, parent controls the modal state
    const isCreatingServiceAccount = externalIsCreating ?? internalIsCreating;
    const setIsCreatingServiceAccount = externalSetIsCreating ?? internalSetIsCreating;

    const {
        loading,
        error,
        data,
        serviceAccounts,
        totalServiceAccounts,
        selectRoleOptions,
        canManageServiceAccounts,
        handleDelete,
        onChangePage,
        refetch,
    } = useServiceAccountListData(page, pageSize, debouncedQuery);

    const { onSelectRole, onCancelRoleAssignment, onConfirmRoleAssignment } = useServiceAccountRoleAssignment(
        selectRoleOptions,
        roleAssignmentState,
        setRoleAssignmentState,
        setOptimisticRoles,
        refetch,
    );

    const handleCreateComplete = () => {
        setIsCreatingServiceAccount(false);
    };

    const handleRoleChange = (serviceAccountUrn: string, newRoleUrn: string, originalRoleUrn: string) => {
        const serviceAccount = serviceAccounts.find((sa) => sa.urn === serviceAccountUrn);
        if (serviceAccount) {
            const displayName = serviceAccount.displayName || serviceAccount.name;
            onSelectRole(serviceAccountUrn, displayName, originalRoleUrn, newRoleUrn);
        }
    };

    if (!canManageServiceAccounts) {
        return (
            <NoPermissionContainer>
                <Icon icon="ShieldWarning" source="phosphor" size="4xl" color="gray" />
                <Text size="lg" weight="semiBold">
                    Access Denied
                </Text>
                <Text size="md" color="gray">
                    You do not have permission to manage service accounts.
                </Text>
            </NoPermissionContainer>
        );
    }

    const getRoleAssignmentMessage = () => {
        if (!roleAssignmentState) return '';
        const roleToAssign = selectRoleOptions.find((role) => role.urn === roleAssignmentState.currentRoleUrn);
        return roleToAssign?.urn === 'urn:li:dataHubRole:NoRole' || !roleToAssign
            ? `Would you like to remove ${roleAssignmentState.serviceAccountName}'s existing role?`
            : `Would you like to assign the role ${roleToAssign?.name} to ${roleAssignmentState.serviceAccountName}?`;
    };

    return (
        <PageContainer>
            {!data && loading && <Message type="loading" content="Loading service accounts..." />}
            {error && <Message type="error" content="Failed to load service accounts! An unexpected error occurred." />}

            <ServiceAccountTable
                query={query}
                setQuery={setQuery}
                setPage={setPage}
                serviceAccounts={serviceAccounts}
                selectRoleOptions={selectRoleOptions}
                optimisticRoles={optimisticRoles}
                loading={loading}
                page={page}
                pageSize={pageSize}
                total={totalServiceAccounts}
                onChangePage={(newPage) => {
                    setPage(onChangePage(newPage));
                }}
                onDelete={handleDelete}
                onRoleChange={handleRoleChange}
                refetch={refetch}
            />

            {isCreatingServiceAccount && (
                <CreateServiceAccountModal
                    visible={isCreatingServiceAccount}
                    onClose={() => setIsCreatingServiceAccount(false)}
                    onCreateServiceAccount={handleCreateComplete}
                />
            )}

            {roleAssignmentState && (
                <Modal
                    open={roleAssignmentState.isViewingAssignRole}
                    title="Confirm Role Assignment"
                    onCancel={onCancelRoleAssignment}
                    footer={
                        <ModalFooter>
                            <Button variant="outline" onClick={onCancelRoleAssignment}>
                                Cancel
                            </Button>
                            <Button variant="filled" onClick={onConfirmRoleAssignment}>
                                Confirm
                            </Button>
                        </ModalFooter>
                    }
                >
                    <Text>{getRoleAssignmentMessage()}</Text>
                </Modal>
            )}
        </PageContainer>
    );
};
