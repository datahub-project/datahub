import { useApolloClient } from '@apollo/client';
import { Empty, List, Pagination, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { useUserContext } from '@app/context/useUserContext';
import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import CreateServiceAccountModal from '@app/identity/serviceAccount/CreateServiceAccountModal';
import ServiceAccountListItem from '@app/identity/serviceAccount/ServiceAccountListItem';
import { removeServiceAccountFromListCache } from '@app/identity/serviceAccount/cacheUtils';
import { SearchBar } from '@app/search/SearchBar';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button } from '@src/alchemy-components';

import { useDeleteServiceAccountMutation, useListServiceAccountsQuery } from '@graphql/auth.generated';
import { useListRolesQuery } from '@graphql/role.generated';
import { DataHubRole, ServiceAccount } from '@types';

const DEFAULT_PAGE_SIZE = 10;

const ServiceAccountContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

const ServiceAccountStyledList = styled(List)`
    display: flex;
    flex-direction: column;
    overflow: auto;
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

export const ServiceAccountList = () => {
    const entityRegistry = useEntityRegistry();
    const apolloClient = useApolloClient();
    const [query, setQuery] = useState<string | undefined>(undefined);
    const [page, setPage] = useState(1);
    const [isCreatingServiceAccount, setIsCreatingServiceAccount] = useState(false);

    const authenticatedUser = useUserContext();
    const canManageServiceAccounts = authenticatedUser?.platformPrivileges?.manageServiceAccounts || false;

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const { loading, error, data, refetch } = useListServiceAccountsQuery({
        skip: !canManageServiceAccounts,
        variables: {
            input: {
                start,
                count: pageSize,
                query: query?.length ? query : undefined,
            },
        },
        fetchPolicy: 'cache-and-network',
    });

    const [deleteServiceAccount] = useDeleteServiceAccountMutation();

    const {
        loading: rolesLoading,
        error: rolesError,
        data: rolesData,
    } = useListRolesQuery({
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                start: 0,
                count: 10,
            },
        },
    });

    const selectRoleOptions = rolesData?.listRoles?.roles?.map((role) => role as DataHubRole) || [];

    const totalServiceAccounts = data?.listServiceAccounts?.total || 0;
    const serviceAccounts = data?.listServiceAccounts?.serviceAccounts || [];

    const handleDelete = async (urn: string) => {
        try {
            await deleteServiceAccount({ variables: { urn } });
            message.success('Service account deleted');
            // Update the cache to remove the deleted service account
            removeServiceAccountFromListCache(apolloClient, urn, page, pageSize);
        } catch (e: any) {
            message.error(`Failed to delete service account: ${e.message || ''}`);
        }
    };

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleCreateComplete = (_urn: string, _name: string, _displayName?: string, _description?: string) => {
        setIsCreatingServiceAccount(false);
        // Cache is updated optimistically by the mutation, no need to refetch immediately
    };

    if (!canManageServiceAccounts) {
        return (
            <ServiceAccountContainer>
                <Empty description="You do not have permission to manage service accounts." />
            </ServiceAccountContainer>
        );
    }

    const isLoading = loading || rolesLoading;
    const hasError = error || rolesError;

    return (
        <>
            {!data && isLoading && <Message type="loading" content="Loading service accounts..." />}
            {hasError && (
                <Message type="error" content="Failed to load service accounts! An unexpected error occurred." />
            )}
            <ServiceAccountContainer>
                <TabToolbar>
                    <div>
                        <Button
                            variant="text"
                            onClick={() => setIsCreatingServiceAccount(true)}
                            data-testid="create-service-account-button"
                            disabled={!canManageServiceAccounts}
                            color="black"
                            icon={{ icon: 'Plus', source: 'phosphor' }}
                        >
                            Create
                        </Button>
                    </div>
                    <SearchBar
                        initialQuery={query || ''}
                        placeholderText="Search service accounts..."
                        suggestions={[]}
                        style={{
                            maxWidth: 220,
                            padding: 0,
                        }}
                        inputStyle={{
                            height: 32,
                            fontSize: 12,
                        }}
                        onSearch={() => null}
                        onQueryChange={(q) => {
                            setPage(1);
                            setQuery(q);
                        }}
                        entityRegistry={entityRegistry}
                        hideRecommendations
                    />
                </TabToolbar>
                <ServiceAccountStyledList
                    bordered
                    locale={{
                        emptyText: <Empty description="No Service Accounts!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    dataSource={serviceAccounts}
                    renderItem={(item: any) => (
                        <ServiceAccountListItem
                            serviceAccount={item as ServiceAccount}
                            selectRoleOptions={selectRoleOptions}
                            onDelete={() => handleDelete(item.urn)}
                            onCreateToken={() => {
                                // Will navigate to token creation
                            }}
                            refetch={refetch}
                        />
                    )}
                />
                <PaginationContainer>
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalServiceAccounts}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </PaginationContainer>
            </ServiceAccountContainer>
            <CreateServiceAccountModal
                visible={isCreatingServiceAccount}
                onClose={() => setIsCreatingServiceAccount(false)}
                onCreateServiceAccount={handleCreateComplete}
            />
        </>
    );
};
