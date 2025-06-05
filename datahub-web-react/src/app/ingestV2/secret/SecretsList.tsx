import { Icon, Pagination, SearchBar, Table, colors } from '@components';
import { Typography, message } from 'antd';
import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import EmptySources from '@app/ingestV2/EmptySources';
import { SecretBuilderModal } from '@app/ingestV2/secret/SecretBuilderModal';
import {
    addSecretToListSecretsCache,
    removeSecretFromListSecretsCache,
    updateSecretInListSecretsCache,
} from '@app/ingestV2/secret/cacheUtils';
import { SecretBuilderState } from '@app/ingestV2/secret/types';
import { scrollToTop } from '@app/shared/searchUtils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import {
    useCreateSecretMutation,
    useDeleteSecretMutation,
    useListSecretsQuery,
    useUpdateSecretMutation,
} from '@graphql/ingestion.generated';

const ButtonsContainer = styled.div`
    display: flex;
    justify-content: end;
    gap: 8px;

    button {
        border: 1px solid ${colors.gray[100]};
        border-radius: 20px;
        width: 24px;
        height: 24px;
        padding: 3px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: none;
        color: ${colors.gray[1800]};

        :hover {
            cursor: pointer;
        }
    }
`;

const SecretsContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: auto;
`;

const StyledTabToolbar = styled(TabToolbar)`
    padding: 0 20px 16px 0;
    height: auto;
    box-shadow: none;
    border-bottom: none;
`;

const SearchContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const StyledSearchBar = styled(SearchBar)`
    width: 400px;
`;

const TableContainer = styled.div`
    flex: 1;
    overflow: auto;
`;

const TextContainer = styled(Typography.Text)`
    color: ${colors.gray[1700]};
`;

type TableDataType = {
    urn: string;
    name: string;
    description: string | null;
};

const DEFAULT_PAGE_SIZE = 25;

interface Props {
    showCreateModal: boolean;
    setShowCreateModal: (show: boolean) => void;
}

export const SecretsList = ({ showCreateModal: isCreatingSecret, setShowCreateModal: setIsCreatingSecret }: Props) => {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const [editSecret, setEditSecret] = useState<SecretBuilderState | undefined>(undefined);
    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);

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
        setShowConfirmDelete(false);
        refetch();
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
                    content: `Failed to update secret!: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };
    const onUpdate = (state: SecretBuilderState, resetBuilderState: () => void) => {
        updateSecretMutation({
            variables: {
                input: {
                    urn: state.urn as string,
                    name: state.name as string,
                    value: state.value as string,
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

    const handleDeleteClose = () => {
        setShowConfirmDelete(false);
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
            render: (record: TableDataType) => (
                <TextContainer
                    ellipsis={{
                        tooltip: {
                            title: record.name,
                            color: 'white',
                            overlayInnerStyle: { color: colors.gray[1700] },
                            showArrow: false,
                        },
                    }}
                >
                    {record.name}
                </TextContainer>
            ),
            sorter: (a: TableDataType, b: TableDataType) => a.name.localeCompare(b.name),
        },
        {
            title: 'Description',
            key: 'description',
            render: (record: TableDataType) => {
                return (
                    <TextContainer
                        ellipsis={{
                            tooltip: {
                                title: record.description,
                                color: 'white',
                                overlayInnerStyle: { color: colors.gray[1700] },
                                showArrow: false,
                            },
                        }}
                    >
                        {record.description || 'No description'}
                    </TextContainer>
                );
            },
            width: '75%',
        },
        {
            title: '',
            key: 'actions',
            render: (record: TableDataType) => (
                <>
                    <ButtonsContainer>
                        <button type="button" onClick={() => onEditSecret(record)} aria-label="Edit secret">
                            <Icon icon="PencilSimpleLine" source="phosphor" />
                        </button>
                        <button
                            type="button"
                            className="delete-action"
                            onClick={() => setShowConfirmDelete(true)}
                            aria-label="Delete secret"
                            data-test-id="delete-secret-action"
                            data-icon="delete"
                        >
                            <Icon icon="Trash" source="phosphor" color="red" />
                        </button>
                    </ButtonsContainer>
                    <ConfirmationModal
                        isOpen={showConfirmDelete}
                        modalTitle="Confirm Secret Removal"
                        modalText="Are you sure you want to remove this secret? Sources that use it may no longer work as expected."
                        handleConfirm={() => deleteSecret(record.urn)}
                        handleClose={handleDeleteClose}
                    />
                </>
            ),
            width: '100px',
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
            {error && message.error({ content: `Failed to load secrets! \n ${error.message || ''}`, duration: 3 })}
            <SecretsContainer>
                <StyledTabToolbar>
                    <SearchContainer>
                        <StyledSearchBar
                            placeholder="Search..."
                            value={query || ''}
                            onChange={(value) => handleSearch(value)}
                        />
                    </SearchContainer>
                </StyledTabToolbar>
                {!loading && totalSecrets === 0 ? (
                    <EmptySources sourceType="secrets" isEmptySearchResult={!!query} />
                ) : (
                    <>
                        <TableContainer>
                            <Table
                                columns={tableColumns}
                                data={tableData}
                                rowKey="urn"
                                isScrollable
                                style={{ tableLayout: 'fixed' }}
                                isLoading={loading}
                            />
                        </TableContainer>
                        <Pagination
                            currentPage={page}
                            itemsPerPage={pageSize}
                            totalPages={totalSecrets}
                            showLessItems
                            onChange={onChangePage}
                            showSizeChanger={false}
                            hideOnSinglePage
                        />
                    </>
                )}
            </SecretsContainer>
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
