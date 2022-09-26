import React, { useEffect, useState } from 'react';
import { Button, Empty, message, Modal, Pagination, Typography } from 'antd';
import { DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import styled from 'styled-components';
import {
    useCreateSecretMutation,
    useDeleteSecretMutation,
    useListSecretsQuery,
} from '../../../graphql/ingestion.generated';
import { Message } from '../../shared/Message';
import TabToolbar from '../../entity/shared/components/styled/TabToolbar';
import { SecretBuilderModal } from './SecretBuilderModal';
import { SecretBuilderState } from './types';
import { StyledTable } from '../../entity/shared/components/styled/StyledTable';
import { SearchBar } from '../../search/SearchBar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { scrollToTop } from '../../shared/searchUtils';

const DeleteButtonContainer = styled.div`
    display: flex;
    justify-content: right;
`;

const SourcePaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const DEFAULT_PAGE_SIZE = 25;

export const SecretsList = () => {
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
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);

    const [deleteSecretMutation] = useDeleteSecretMutation();
    const [createSecretMutation] = useCreateSecretMutation();
    const { loading, error, data, refetch } = useListSecretsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query,
            },
        },
        fetchPolicy: 'no-cache',
    });

    const totalSecrets = data?.listSecrets?.total || 0;
    const secrets = data?.listSecrets?.secrets || [];
    const filteredSecrets = secrets.filter((user) => !removedUrns.includes(user.urn));

    const deleteSecret = async (urn: string) => {
        deleteSecretMutation({
            variables: { urn },
        })
            .then(() => {
                message.success({ content: 'Removed secret.', duration: 2 });
                const newRemovedUrns = [...removedUrns, urn];
                setRemovedUrns(newRemovedUrns);
                setTimeout(function () {
                    refetch?.();
                }, 3000);
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
                    content: `Successfully created Secret!`,
                    duration: 3,
                });
                resetBuilderState();
                setIsCreatingSecret(false);
                setTimeout(() => refetch(), 3000);
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to update ingestion source!: \n ${e.message || ''}`,
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
                    <Button onClick={() => onDeleteSecret(record.urn)} type="text" shape="circle" danger>
                        <DeleteOutlined />
                    </Button>
                </DeleteButtonContainer>
            ),
        },
    ];

    const tableData = filteredSecrets?.map((secret) => ({
        urn: secret.urn,
        name: secret.name,
        description: secret.description,
    }));

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading secrets..." />}
            {error && message.error({ content: `Failed to load secrets! \n ${error.message || ''}`, duration: 3 })}
            <div>
                <TabToolbar>
                    <div>
                        <Button type="text" onClick={() => setIsCreatingSecret(true)}>
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
                        onQueryChange={(q) => setQuery(q)}
                        entityRegistry={entityRegistry}
                        hideRecommendations
                    />
                </TabToolbar>
                <StyledTable
                    columns={tableColumns}
                    dataSource={tableData}
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
                visible={isCreatingSecret}
                onSubmit={onSubmit}
                onCancel={() => setIsCreatingSecret(false)}
            />
        </>
    );
};
