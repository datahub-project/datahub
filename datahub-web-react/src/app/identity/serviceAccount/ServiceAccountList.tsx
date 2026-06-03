import { ShieldWarning } from '@phosphor-icons/react/dist/csr/ShieldWarning';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import CreateServiceAccountModal from '@app/identity/serviceAccount/CreateServiceAccountModal';
import { ModalFooter, ServiceAccountTable } from '@app/identity/serviceAccount/ServiceAccountList.components';
import {
    useServiceAccountDefaultView,
    useServiceAccountListData,
    useServiceAccountListState,
    useServiceAccountRoleAssignment,
    useServiceAccountViewOptions,
} from '@app/identity/serviceAccount/ServiceAccountList.hooks';
import { Message } from '@app/shared/Message';
import { Button, Icon, Modal, Text } from '@src/alchemy-components';

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
    color: ${(props) => props.theme.colors.textSecondary};
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
    const { t } = useTranslation('entity.identity');
    const { t: tc } = useTranslation('common.actions');

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

    const { viewOptions } = useServiceAccountViewOptions();
    const { handleDefaultViewChange } = useServiceAccountDefaultView(refetch);

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
                <Icon icon={ShieldWarning} size="4xl" color="gray" />
                <Text size="lg" weight="semiBold">
                    {t('serviceAccounts.accessDenied')}
                </Text>
                <Text size="md" color="gray">
                    {t('serviceAccounts.noPermission')}
                </Text>
            </NoPermissionContainer>
        );
    }

    const getRoleAssignmentMessage = () => {
        if (!roleAssignmentState) return '';
        const roleToAssign = selectRoleOptions.find((role) => role.urn === roleAssignmentState.currentRoleUrn);
        return roleToAssign?.urn === 'urn:li:dataHubRole:NoRole' || !roleToAssign
            ? t('roleAssignment.removeMessage', { name: roleAssignmentState.serviceAccountName })
            : t('roleAssignment.assignMessage', {
                  role: roleToAssign?.name,
                  name: roleAssignmentState.serviceAccountName,
              });
    };

    return (
        <PageContainer>
            {!data && loading && <Message type="loading" content={t('serviceAccounts.loading')} />}
            {error && <Message type="error" content={t('serviceAccounts.loadError')} />}

            <ServiceAccountTable
                query={query}
                setQuery={setQuery}
                setPage={setPage}
                serviceAccounts={serviceAccounts}
                selectRoleOptions={selectRoleOptions}
                optimisticRoles={optimisticRoles}
                viewOptions={viewOptions}
                loading={loading}
                page={page}
                pageSize={pageSize}
                total={totalServiceAccounts}
                onChangePage={(newPage) => {
                    setPage(onChangePage(newPage));
                }}
                onDelete={handleDelete}
                onRoleChange={handleRoleChange}
                onDefaultViewChange={handleDefaultViewChange}
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
                    title={t('roleAssignment.confirmTitle')}
                    onCancel={onCancelRoleAssignment}
                    footer={
                        <ModalFooter>
                            <Button variant="outline" onClick={onCancelRoleAssignment}>
                                {tc('cancel')}
                            </Button>
                            <Button variant="filled" onClick={onConfirmRoleAssignment}>
                                {tc('confirm')}
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
