import { SearchBar } from '@components';
import { Empty, Modal, Typography, message } from 'antd';
import { PencilSimpleLine, Trash } from 'phosphor-react';
import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import { SecretBuilderModal } from '@app/ingest/secret/SecretBuilderModal';
import {
    addSecretToListSecretsCache,
    removeSecretFromListSecretsCache,
    updateSecretInListSecretsCache,
} from '@app/ingest/secret/cacheUtils';
import { SecretBuilderState } from '@app/ingest/secret/types';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { Pagination, Table } from '@src/alchemy-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import {
    useCreateSecretMutation,
    useDeleteSecretMutation,
    useListSecretsQuery,
    useUpdateSecretMutation,
} from '@graphql/ingestion.generated';

const DeleteButtonContainer = styled.div`
    display: flex;
    justify-content: right;
    gap: 8px;

    button {
        border: 1px solid #d9d9d9;
        border-radius: 20px;
        width: 28px;
        height: 28px;
        padding: 4px;
        color: #595959;
        display: flex;
        align-items: center;
        justify-content: center;
        background: none;
        cursor: pointer;
        :hover {
            color: #262626;
            border-color: #262626;
        }

        &.delete-action {
            color: #ff4d4f;
            :hover {
                color: #cf1322;
                border-color: #262626;
            }
        }
    }
`;

const EmptyState = () => (
    <div style={{ padding: '20px', textAlign: 'center' }}>
        <Empty description="No Secrets found!" image={Empty.PRESENTED_IMAGE_SIMPLE} />
    </div>
);

type TableDataType = {
    urn: string;
    name: string;
    description: string | null;
};

const DEFAULT_PAGE_SIZE = 25;

type Props = {
    showCreateModal: boolean;
    setShowCreateModal: (show: boolean) => void;
};

const SearchContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const StyledTabToolbar = styled(TabToolbar)`
    padding: 16px 20px;
    height: auto;
    &&& {
        padding: 8px 20px;
        height: auto;
        box-shadow: none;
        border-bottom: none;
    }
`;

const StyledSearchBar = styled(SearchBar)`
    width: 220px;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    margin-top: 16px;
`;

const TableContainer = styled.div`
    padding-left: 16px;
`;

export const SecretsList = ({ showCreateModal: isCreatingSecret, setShowCreateModal: setIsCreatingSecret }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const [editSecret, setEditSecret] = useState<SecretBuilderState | undefined>(undefined);

    const [deleteSecretMutation] = useDeleteSecretMutation();
    const [createSecretMutation] = useCreateSecretMutation();
    const [updateSecretMutation] = useUpdateSecretMutation();
    const { loading, error, data, client, refetch } = useListSecretsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query: (query?.length && query) || undefined,
            },
        },
        fetchPolicy: (query?.length || 0) > 0 ? 'no-cache' : 'cache-first',
    });

    const totalSecrets = data?.listSecrets?.total || 0;
    const secrets = data?.listSecrets?.secrets || [];

    const deleteSecret = (urn: string) => {
        deleteSecretMutation({
            variables: { urn },
        })
            .then(() => {
                message.success({ content: 'Removed secret.', duration: 2 });
                removeSecretFromListSecretsCache(urn, client, page, pageSize);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to remove secret: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleSearch = (value: string) => {
        setPage(1);
        setQuery(value);
    };

    const onSubmit = (state: SecretBuilderState, resetBuilderState: () => void) => {
        createSecretMutation({
            variables: {
                input: {
                    name: state.name as string,
                    value: state.value as string,
                    description: state.description as string,
                },
            },
        })
            .then((res) => {
                message.success({
                    content: `Successfully created Secret!`,
                    duration: 3,
                });
                resetBuilderState();
                setIsCreatingSecret(false);
                addSecretToListSecretsCache(
                    {
                        urn: res.data?.createSecret || '',
                        name: state.name,
                        description: state.description || '',
                    },
                    client,
                    pageSize,
                );
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to update Secret!: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    const onUpdate = (state: SecretBuilderState, resetBuilderState: () => void) => {
        const secretValue = state.value || '';

        updateSecretMutation({
            variables: {
                input: {
                    urn: state.urn as string,
                    name: state.name as string,
                    value: secretValue,
                    description: state.description as string,
                },
            },
        })
            .then(() => {
                message.success({
                    content: `Successfully updated Secret!`,
                    duration: 3,
                });
                resetBuilderState();
                setIsCreatingSecret(false);
                setEditSecret(undefined);
                updateSecretInListSecretsCache(
                    {
                        urn: state.urn,
                        name: state.name,
                        description: state.description,
                    },
                    client,
                    pageSize,
                    page,
                );
                setTimeout(() => {
                    refetch();
                }, 3000);
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to update Secret!: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    const onDeleteSecret = (urn: string) => {
        Modal.confirm({
            title: `Confirm Secret Removal`,
            content: `Are you sure you want to remove this secret? Sources that use it may no longer work as expected.`,
            onOk() {
                deleteSecret(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const onEditSecret = (urnData: any) => {
        setIsCreatingSecret(true);
        setEditSecret(urnData);
    };

    const onCancel = () => {
        setIsCreatingSecret(false);
        setEditSecret(undefined);
    };

    const tableColumns = [
        {
            title: 'Name',
            key: 'name',
            render: (record: TableDataType) => <Typography.Text strong>{record.name}</Typography.Text>,
            sorter: (a: TableDataType, b: TableDataType) => a.name.localeCompare(b.name),
        },
        {
            title: 'Description',
            key: 'description',
            render: (record: TableDataType) => {
                return <>{record.description || <Typography.Text type="secondary">No description</Typography.Text>}</>;
            },
        },
        {
            title: '',
            key: 'actions',
            render: (record: TableDataType) => (
                <DeleteButtonContainer>
                    <button type="button" onClick={() => onEditSecret(record)} aria-label="Edit secret">
                        <PencilSimpleLine size={16} />
                    </button>
                    <button
                        type="button"
                        className="delete-action"
                        onClick={() => onDeleteSecret(record.urn)}
                        aria-label="Delete secret"
                        data-test-id="delete-secret-action"
                        data-icon="delete"
                    >
                        <Trash size={16} />
                    </button>
                </DeleteButtonContainer>
            ),
        },
    ];

    const tableData =
        secrets?.map((secret) => ({
            urn: secret.urn,
            name: secret.name,
            description: secret.description || null,
        })) || [];

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading secrets..." />}
            {error && message.error({ content: `Failed to load secrets! \n ${error.message || ''}`, duration: 3 })}
            <div>
                <StyledTabToolbar>
                    <SearchContainer>
                        <StyledSearchBar
                            placeholder="Search..."
                            value={query || ''}
                            onChange={(value) => handleSearch(value)}
                        />
                    </SearchContainer>
                </StyledTabToolbar>
                {tableData.length === 0 ? (
                    <EmptyState />
                ) : (
                    <>
                        <TableContainer>
                            <Table
                                columns={tableColumns}
                                data={tableData}
                                rowKey="urn"
                                isScrollable={isShowNavBarRedesign}
                                maxHeight={isShowNavBarRedesign ? 'calc(100vh - 450px)' : undefined}
                                showHeader
                            />
                        </TableContainer>
                        <PaginationContainer>
                            <Pagination
                                currentPage={page}
                                itemsPerPage={pageSize}
                                total={totalSecrets}
                                onPageChange={onChangePage}
                            />
                        </PaginationContainer>
                    </>
                )}
            </div>
            <SecretBuilderModal
                open={isCreatingSecret}
                editSecret={editSecret}
                onUpdate={onUpdate}
                onSubmit={onSubmit}
                onCancel={onCancel}
            />
        </>
    );
};
