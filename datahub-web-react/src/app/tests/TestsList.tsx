import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';
import { Button, Empty, message, Modal, Pagination, Typography } from 'antd';
import { DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import * as QueryString from 'query-string';
import {
    useCreateTestMutation,
    useDeleteTestMutation,
    useListTestsQuery,
    useUpdateTestMutation,
} from '../../graphql/test.generated';
import { Message } from '../shared/Message';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import { TestBuilderModal } from './builder/TestBuilderModal';
import { TestBuilderState } from './types';
import { StyledTable } from '../entity/shared/components/styled/StyledTable';
import { SearchBar } from '../search/SearchBar';
import { useEntityRegistry } from '../useEntityRegistry';
import { TestResultsSummary } from './TestResultsSummary';
import CopyUrn from '../shared/CopyUrn';

const DeleteButtonContainer = styled.div`
    display: flex;
    justify-content: right;
`;

const SourcePaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const DEFAULT_PAGE_SIZE = 25;

export const TestsList = () => {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();

    const [page, setPage] = useState(1);

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const [isBuildingTest, setIsBuildingTest] = useState<boolean>(false);
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);
    const [focusTestUrn, setFocusTestUrn] = useState<undefined | string>(undefined);
    const [isEditing, setIsEditing] = useState<boolean>(false);
    const [query, setQuery] = useState<undefined | string>(undefined);

    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [deleteTestMutation] = useDeleteTestMutation();
    const [updateTestMutation] = useUpdateTestMutation();
    const [createTestMutation] = useCreateTestMutation();
    const { loading, error, data, refetch } = useListTestsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query,
                // filters: null,
            },
        },
        fetchPolicy: 'no-cache',
    });

    const totalTests = data?.listTests?.total || 0;
    const tests = data?.listTests?.tests || [];
    const filteredTests = tests.filter((user) => !removedUrns.includes(user.urn));
    const focusTest = (focusTestUrn && filteredTests.find((test) => test.urn === focusTestUrn)) || undefined;

    const deleteTest = async (urn: string) => {
        deleteTestMutation({
            variables: { urn },
        })
            .then(() => {
                message.success({ content: 'Removed test.', duration: 2 });
                const newRemovedUrns = [...removedUrns, urn];
                setRemovedUrns(newRemovedUrns);
                setTimeout(function () {
                    refetch?.();
                }, 3000);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to remove test: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const onSubmit = (state: TestBuilderState, resetBuilderState: () => void) => {
        const input = {
            name: state.name as string,
            category: state.category as string,
            description: state.description as string,
            definition: {
                ...state.definition,
            },
        };
        if (isEditing && focusTestUrn) {
            updateTestMutation({
                variables: {
                    urn: focusTestUrn,
                    input,
                },
            })
                .then(() => {
                    message.success({
                        content: `Successfully edited Test!`,
                        duration: 3,
                    });
                    resetBuilderState();
                    setIsBuildingTest(false);
                    setTimeout(() => refetch(), 3000);
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to edit Test!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                });
        } else {
            createTestMutation({
                variables: {
                    input,
                },
            })
                .then(() => {
                    message.success({
                        content: `Successfully created Test!`,
                        duration: 3,
                    });
                    resetBuilderState();
                    setIsBuildingTest(false);
                    setTimeout(() => refetch(), 3000);
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to create Test!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                });
        }
    };

    const onDeleteTest = (urn: string) => {
        Modal.confirm({
            title: `Confirm Test Removal`,
            content: `Are you sure you want to remove this test? This test will no longer be evaluated on your assets.`,
            onOk() {
                deleteTest(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const onEdit = (urn: string) => {
        console.log('setting on edit true');
        setIsEditing(true);
        setIsBuildingTest(true);
        setFocusTestUrn(urn);
    };

    const tableColumns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (name: string) => <Typography.Text strong>{name}</Typography.Text>,
        },
        {
            title: 'Category',
            dataIndex: 'category',
            key: 'category',
            render: (category: any) => {
                return (
                    <>
                        <Typography.Text type="secondary">{category}</Typography.Text>
                    </>
                );
            },
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
            title: 'Results',
            dataIndex: '',
            key: '',
            render: (_: any, record: any) => {
                return (
                    <>
                        <TestResultsSummary urn={record.urn} />
                    </>
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <DeleteButtonContainer>
                    <Button style={{ marginRight: 16 }} onClick={() => onEdit(record.urn)}>
                        Edit
                    </Button>
                    <CopyUrn urn={record.urn} />
                    <Button onClick={() => onDeleteTest(record.urn)} type="text" shape="circle" danger>
                        <DeleteOutlined />
                    </Button>
                </DeleteButtonContainer>
            ),
        },
    ];

    const tableData = filteredTests?.map((test) => ({
        urn: test.urn,
        name: test.name,
        category: test.category,
        description: test.description,
    }));

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading tests..." />}
            {error && message.error({ content: `Failed to load tests! \n ${error.message || ''}`, duration: 3 })}
            <div>
                <TabToolbar>
                    <div>
                        <Button type="text" onClick={() => setIsBuildingTest(true)}>
                            <PlusOutlined /> Create new test
                        </Button>
                    </div>
                    <SearchBar
                        initialQuery=""
                        placeholderText="Search tests..."
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
                    />
                </TabToolbar>
                <StyledTable
                    columns={tableColumns}
                    dataSource={tableData}
                    rowKey="urn"
                    locale={{
                        emptyText: <Empty description="No Tests found!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    pagination={false}
                />
                <SourcePaginationContainer>
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalTests}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </SourcePaginationContainer>
            </div>
            <TestBuilderModal
                initialState={focusTest}
                editing={isEditing}
                visible={isBuildingTest}
                onSubmit={onSubmit}
                onCancel={() => setIsBuildingTest(false)}
            />
        </>
    );
};
