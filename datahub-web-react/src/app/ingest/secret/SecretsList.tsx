import React, { useEffect, useState } from 'react';
import { Button, Empty, message, Modal, Pagination, Typography } from 'antd';
import { debounce } from 'lodash';
import { DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
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
import { StyledTable } from '../../entity/shared/components/styled/StyledTable';
import { SearchBar } from '../../search/SearchBar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { scrollToTop } from '../../shared/searchUtils';
import {
    addSecretToListSecretsCache,
    removeSecretFromListSecretsCache,
    updateSecretInListSecretsCache,
} from './cacheUtils';
import { ONE_SECOND_IN_MS } from '../../entity/shared/tabs/Dataset/Queries/utils/constants';

const DeleteButtonContainer = styled.div`
    display: flex;
    justify-content: right;
`;

const SourcePaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const StyledTableWithNavBarRedesign = styled(StyledTable)`
    overflow: hidden;

    &&& .ant-table-body {
        overflow-y: auto;
        height: calc(100vh - 450px);
    }
` as typeof StyledTable;

const DEFAULT_PAGE_SIZE = 25;

export const SecretsList = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    // Whether or not there is an urn to show in the modal
    const [isCreatingSecret, setIsCreatingSecret] = useState<boolean>(false);
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

    const debouncedSetQuery = debounce((newQuery: string | undefined) => {
        setQuery(newQuery);
    }, ONE_SECOND_IN_MS);

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
                }, 2000);
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
            dataIndex: 'name',
            key: 'name',
            render: (name: string) => <Typography.Text strong>{name}</Typography.Text>,
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            render: (description: any) => {
                return <>{description || <Typography.Text type="secondary">No description</Typography.Text>}</>;
            },
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <DeleteButtonContainer>
                    <Button style={{ marginRight: 16 }} onClick={() => onEditSecret(record)}>
                        EDIT
                    </Button>
                    <Button onClick={() => onDeleteSecret(record.urn)} type="text" shape="circle" danger>
                        <DeleteOutlined />
                    </Button>
                </DeleteButtonContainer>
            ),
        },
    ];

    const tableData = secrets?.map((secret) => ({
        urn: secret.urn,
        name: secret.name,
        description: secret.description,
    }));

    const FinalStyledTable = isShowNavBarRedesign ? StyledTableWithNavBarRedesign : StyledTable;

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading secrets..." />}
            {error && message.error({ content: `Failed to load secrets! \n ${error.message || ''}`, duration: 3 })}
            <div>
                <TabToolbar>
                    <div>
                        <Button
                            data-testid="create-secret-button"
                            type="text"
                            onClick={() => setIsCreatingSecret(true)}
                        >
                            <PlusOutlined /> Create new secret
                        </Button>
                    </div>
                    <SearchBar
                        initialQuery={query || ''}
                        placeholderText="Search secrets..."
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
                            debouncedSetQuery(q);
                        }}
                        entityRegistry={entityRegistry}
                        hideRecommendations
                    />
                </TabToolbar>
                <FinalStyledTable
                    columns={tableColumns}
                    dataSource={tableData}
                    scroll={isShowNavBarRedesign ? { y: 'max-content' } : {}}
                    rowKey="urn"
                    locale={{
                        emptyText: <Empty description="No Secrets found!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    pagination={false}
                />
                <SourcePaginationContainer>
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalSecrets}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </SourcePaginationContainer>
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
