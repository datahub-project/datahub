import React, { useEffect, useState } from 'react';
import { Empty, message, Modal, Typography, Pagination } from 'antd';
import { PencilSimpleLine, Trash } from 'phosphor-react';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import styled from 'styled-components';
import { SearchBar, Table, colors } from '@components';
import {
    useCreateSecretMutation,
    useDeleteSecretMutation,
    useListSecretsQuery,
    useUpdateSecretMutation,
} from '../../../graphql/ingestion.generated';
import { Message } from '../../shared/Message';
import TabToolbar from '../../entity/shared/components/styled/TabToolbar';
import { SecretBuilderModal } from './SecretBuilderModal';
import { SecretBuilderState } from './types';
import { scrollToTop } from '../../shared/searchUtils';

const DeleteButtonContainer = styled.div`
    display: flex;
    justify-content: right;
    gap: 8px;

    button {
        border: none;
        width: 28px;
        height: 28px;
        padding: 4px;
        color: ${colors.gray[1800]};
        display: flex;
        align-items: center;
        justify-content: center;
        background: none;
        cursor: pointer;
        :hover {
            color: #262626;
        }

        &.delete-action {
            color: #ff4d4f;
            :hover {
                color: #cf1322;
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

interface SecretsListProps {
    showCreateModal: boolean;
    setShowCreateModal: (show: boolean) => void;
}

const SearchContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const StyledTabToolbar = styled(TabToolbar)`
    &&& {
        padding: 8px 0;
        height: auto;
        box-shadow: none;
        border-bottom: none;
        margin: 0;
    }
`;

const StyledSearchBar = styled(SearchBar)`
    width: 220px;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    padding: 16px 0;
`;

const TableContainer = styled.div`
    height: calc(100vh - 350px);
    min-height: 200px;
`;

const ContentContainer = styled.div``;

export const SecretsList = ({
    showCreateModal: isCreatingSecret,
    setShowCreateModal: setIsCreatingSecret,
}: SecretsListProps) => {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const paramsPage = (params?.page as string) || undefined;

    const [page, setPage] = useState(1);
    const [query, setQuery] = useState<string | undefined>(undefined);
    const [editSecret, setEditSecret] = useState<SecretBuilderState | undefined>(undefined);

    const { data, loading, error, refetch } = useListSecretsQuery({
        variables: {
            input: {
                start: (page - 1) * DEFAULT_PAGE_SIZE,
                count: DEFAULT_PAGE_SIZE,
                query: query || undefined,
            },
        },
    });

    const [deleteSecretMutation] = useDeleteSecretMutation();
    const [createSecretMutation] = useCreateSecretMutation();
    const [updateSecretMutation] = useUpdateSecretMutation();

    const secrets = data?.listSecrets?.secrets || [];
    const totalSecrets = data?.listSecrets?.total || 0;

    useEffect(() => {
        if (paramsPage) {
            setPage(parseInt(paramsPage, 10));
        }
        if (paramsQuery) {
            setQuery(paramsQuery);
        }
    }, [paramsPage, paramsQuery]);

    useEffect(() => {
        scrollToTop();
    }, [page]);

    const onChangePage = (newPage: number) => {
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
            .then(() => {
                message.success({
                    content: 'Successfully created Secret!',
                    duration: 3,
                });
                resetBuilderState();
                setIsCreatingSecret(false);
                refetch();
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to create secret!: \n ${e.message || ''}`,
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
                    content: 'Successfully updated Secret!',
                    duration: 3,
                });
                resetBuilderState();
                setEditSecret(undefined);
                refetch();
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
            title: 'Confirm Secret Removal',
            content: 'Are you sure you want to remove this secret?',
            onOk() {
                deleteSecretMutation({
                    variables: {
                        urn,
                    },
                })
                    .then(() => {
                        message.success({
                            content: 'Successfully removed Secret!',
                            duration: 3,
                        });
                        refetch();
                    })
                    .catch((e) => {
                        message.destroy();
                        message.error({
                            content: `Failed to remove secret!: \n ${e.message || ''}`,
                            duration: 3,
                        });
                    });
            },
        });
    };

    const onEditSecret = (record: TableDataType) => {
        setEditSecret({
            urn: record.urn,
            name: record.name,
            description: record.description || '',
            value: '',
        });
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
                return record.description || '';
            },
        },
        {
            title: '',
            key: 'actions',
            render: (record: TableDataType) => (
                <DeleteButtonContainer>
                    <button
                        type="button"
                        onClick={() => onEditSecret(record)}
                        aria-label="Edit secret"
                        data-testid="edit-secret-action"
                    >
                        <PencilSimpleLine size={16} />
                    </button>
                    <button
                        type="button"
                        className="delete-action"
                        onClick={() => onDeleteSecret(record.urn)}
                        aria-label="Delete secret"
                        data-testid="delete-secret-action"
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
            <ContentContainer>
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
                                isScrollable
                                maxHeight="100%"
                                showHeader
                            />
                        </TableContainer>
                        <PaginationContainer>
                            <Pagination
                                current={page}
                                pageSize={DEFAULT_PAGE_SIZE}
                                total={totalSecrets}
                                showLessItems
                                onChange={onChangePage}
                                showSizeChanger={false}
                            />
                        </PaginationContainer>
                    </>
                )}
            </ContentContainer>
            <SecretBuilderModal
                open={isCreatingSecret}
                onSubmit={onSubmit}
                onCancel={() => setIsCreatingSecret(false)}
            />
            {editSecret && (
                <SecretBuilderModal
                    open={!!editSecret}
                    onSubmit={onUpdate}
                    onCancel={() => setEditSecret(undefined)}
                    initialState={editSecret}
                />
            )}
        </>
    );
};
