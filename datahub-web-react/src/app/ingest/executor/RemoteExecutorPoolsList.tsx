import TabToolbar from '@src/app/entityV2/shared/components/styled/TabToolbar';
import { Message } from '@src/app/shared/Message';
import {
    useListRemoteExecutorPoolsQuery,
    useUpdateDefaultRemoteExecutorPoolMutation,
} from '@src/graphql/remote_executor.generated';
import { Button, Pagination } from 'antd';
import { ArrowClockwise, Plus } from 'phosphor-react';
import React, { useState } from 'react';
import styled from 'styled-components';
import { RemoteExecutorPool } from '@src/types.generated';
import { RemoteExecutorPoolsTable } from './RemoteExecutorPoolsTable';
import CreateRemoteExecutorPoolModal from './CreateRemoteExecutorPoolModal';

const DEFAULT_PAGE_SIZE = 25;
const REMOTE_EXECUTORS_CREATE_SOURCE_ID = 'REMOTE_EXECUTORS_CREATE_SOURCE_ID';
const REMOTE_EXECUTORS_REFRESH_SOURCE_ID = 'REMOTE_EXECUTORS_REFRESH_SOURCE_ID';

const ExecutorsContainer = styled.div``;

const PlusStyled = styled(Plus)`
    position: relative;
    top: 2px;
`;
const ArrowClockwiseStyled = styled(ArrowClockwise)`
    position: relative;
    top: 2px;
`;

export const RemoteExecutorPoolsList = () => {
    // ---------------------- loading data ---------------------- //
    const [page, setPage] = useState(1);
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const { loading, error, data, refetch } = useListRemoteExecutorPoolsQuery({
        variables: {
            start,
            count: pageSize,
        },
        fetchPolicy: 'no-cache',
        pollInterval: 10000,
    });
    const total = data?.listRemoteExecutorPools.total;
    const remoteExecutorPools = (data?.listRemoteExecutorPools.remoteExecutorPools ?? []) as RemoteExecutorPool[];

    const [isRefreshing, setIsRefreshing] = useState(false);
    const onRefresh = () => {
        setIsRefreshing(true);
        refetch();
        setTimeout(() => setIsRefreshing(false), 500);
    };
    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    // ---------------------- updating default pools ---------------------- //
    const [updateDefaultPoolMutation] = useUpdateDefaultRemoteExecutorPoolMutation();
    const updateDefaultPool = (urn: string) => {
        updateDefaultPoolMutation({ variables: { urn } }).then(onRefresh);
    };

    // ---------------------- create pools ---------------------- //
    const [showCreatePoolModal, setShowCreatePoolModal] = useState(false);
    const onCreatePool = () => {
        setShowCreatePoolModal(true);
    };

    // ---------------------- render ---------------------- //
    return (
        <>
            {/* ----------- Loading and error messages ----------- */}
            {((loading && !data) || isRefreshing) && <Message type="loading" content="Loading executors..." />}
            {error && <Message type="error" content="Failed to load executors! An unexpected error occurred." />}
            {/* ----------- Main content ----------- */}
            <ExecutorsContainer>
                {/* ----------- Toolbar ----------- */}
                <TabToolbar>
                    <Button id={REMOTE_EXECUTORS_CREATE_SOURCE_ID} type="text" onClick={onCreatePool}>
                        <PlusStyled />
                        <span style={{ marginLeft: 4 }}>Create</span>
                    </Button>
                    <Button id={REMOTE_EXECUTORS_REFRESH_SOURCE_ID} type="text" onClick={onRefresh}>
                        <ArrowClockwiseStyled />
                        <span style={{ marginLeft: 4 }}>Refresh</span>
                    </Button>
                </TabToolbar>
                {/* ----------- Table ----------- */}
                <RemoteExecutorPoolsTable
                    pools={remoteExecutorPools}
                    onRefresh={onRefresh}
                    updateDefaultPool={updateDefaultPool}
                />
                {/* ----------- Pagination ----------- */}
                <Pagination
                    style={{ margin: 16 }}
                    current={page}
                    pageSize={pageSize}
                    total={total}
                    showLessItems
                    onChange={onChangePage}
                    showSizeChanger={false}
                />
            </ExecutorsContainer>
            {showCreatePoolModal && (
                <CreateRemoteExecutorPoolModal
                    visible={showCreatePoolModal}
                    onCancel={() => setShowCreatePoolModal(false)}
                    onSuccessfulCreate={() => setTimeout(onRefresh, 500)}
                />
            )}
        </>
    );
};
