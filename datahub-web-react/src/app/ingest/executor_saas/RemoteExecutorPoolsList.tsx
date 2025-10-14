// SaaS only
import { Button, SearchBar } from '@components';
import { Pagination } from 'antd';
import { ArrowClockwise } from 'phosphor-react';
import * as QueryString from 'query-string';
import React, { useEffect, useRef, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { INGESTION_TAB_QUERY_PARAMS } from '@app/ingest/constants';
import CreateRemoteExecutorPoolModal from '@app/ingest/executor_saas/CreateRemoteExecutorPoolModal';
import { RemoteExecutorPoolProvisioningPreviewModal } from '@app/ingest/executor_saas/RemoteExecutorPoolProvisioningPreviewModal';
import { RemoteExecutorPoolsTable } from '@app/ingest/executor_saas/RemoteExecutorPoolsTable';
import { TabType } from '@app/ingest/types';
import TabToolbar from '@src/app/entityV2/shared/components/styled/TabToolbar';
import { useQueryParamValue } from '@src/app/entityV2/shared/useQueryParamValue';
import { Message } from '@src/app/shared/Message';
import {
    useGetRemoteExecutorPoolQuery,
    useListRemoteExecutorPoolsQuery,
    useUpdateDefaultRemoteExecutorPoolMutation,
} from '@src/graphql/remote_executor.saas.generated';
import { RemoteExecutorPool } from '@src/types.generated';

const DEFAULT_PAGE_SIZE = 25;
export const REMOTE_EXECUTORS_CREATE_SOURCE_ID = 'REMOTE_EXECUTORS_CREATE_SOURCE_ID';
const REMOTE_EXECUTORS_REFRESH_SOURCE_ID = 'REMOTE_EXECUTORS_REFRESH_SOURCE_ID';

const ExecutorsContainer = styled.div``;

const ArrowClockwiseStyled = styled(ArrowClockwise)`
    position: relative;
    top: 2px;
`;

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

const RefreshButtonContainer = styled.div`
    flex-direction: row;
    display: flex;
    gap: 24px;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    margin-top: 16px;
`;

type Props = {
    onSwitchTab: (tab: string) => void;
    showCreatePoolModal: boolean;
    setShowCreatePoolModal: (show: boolean) => void;
};

export const RemoteExecutorPoolsList = ({ onSwitchTab, showCreatePoolModal, setShowCreatePoolModal }: Props) => {
    const defaultQuery = useQueryParamValue('pool');

    // ---------------------- load & search data ---------------------- //
    const [query, setQuery] = useState<string | undefined>(typeof defaultQuery === 'string' ? defaultQuery : undefined);
    const [page, setPage] = useState(1);
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const { loading, error, data, refetch } = useListRemoteExecutorPoolsQuery({
        variables: {
            start,
            count: pageSize,
            query,
        },
        fetchPolicy: 'no-cache',
        pollInterval: 10000,
    });
    const total = data?.listRemoteExecutorPools?.total;
    const remoteExecutorPools = (data?.listRemoteExecutorPools?.remoteExecutorPools ?? []) as RemoteExecutorPool[];

    const [isRefreshing, setIsRefreshing] = useState(false);
    const onRefresh = () => {
        setIsRefreshing(true);
        refetch();
        setTimeout(() => setIsRefreshing(false), 500);
    };
    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const handleSearch = (value: string) => {
        setPage(1);
        setQuery(value);
    };

    // ---------------------- updating default pools ---------------------- //
    const [updateDefaultPoolMutation] = useUpdateDefaultRemoteExecutorPoolMutation();
    const updateDefaultPool = (urn: string) => {
        updateDefaultPoolMutation({ variables: { urn } }).then(onRefresh);
    };

    // ---------------------- view pool provisioning status ---------------------- //
    const [showPoolProvisioningStatusForUrn, setShowPoolProvisioningStatusForUrn] = useState<string | null>(null);

    const { data: poolProvisioningStatus, refetch: refetchPoolProvisioningStatus } = useGetRemoteExecutorPoolQuery({
        variables: {
            urn: showPoolProvisioningStatusForUrn || '',
        },
        skip: !showPoolProvisioningStatusForUrn,
    });

    const onViewPoolProvisioningStatus = (urn: string) => {
        setShowPoolProvisioningStatusForUrn(urn);
    };

    // ---------------------- default search handling ---------------------- //
    const searchInputRef = useRef<HTMLInputElement>(null);
    useEffect(() => {
        if (defaultQuery?.length) {
            searchInputRef.current?.focus();
        }
    }, [defaultQuery]);

    // ---------------------- open source tab with parameters ---------------------- //
    const history = useHistory();
    const location = useLocation();
    const onViewSourcesForPool = (pool: string) => {
        // first, add pool to query parameters
        const newParams = { [INGESTION_TAB_QUERY_PARAMS.pool]: pool };
        const newSearch = QueryString.stringify(newParams);
        history.push(`${location.pathname}${newSearch ? `?${newSearch}` : ''}`);
        // then, switch to the correct tab
        onSwitchTab(TabType.Sources);
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
                <StyledTabToolbar>
                    <SearchContainer>
                        <StyledSearchBar placeholder="Search pools..." value={query || ''} onChange={handleSearch} />
                    </SearchContainer>
                    <RefreshButtonContainer>
                        <Button id={REMOTE_EXECUTORS_REFRESH_SOURCE_ID} variant="text" onClick={onRefresh}>
                            <ArrowClockwiseStyled /> <span style={{ marginLeft: 4 }}>Refresh</span>
                        </Button>
                    </RefreshButtonContainer>
                </StyledTabToolbar>
                {/* ----------- Table ----------- */}
                <RemoteExecutorPoolsTable
                    pools={remoteExecutorPools}
                    onRefresh={onRefresh}
                    updateDefaultPool={updateDefaultPool}
                    viewSourcesForPool={onViewSourcesForPool}
                    viewPoolProvisioningStatus={onViewPoolProvisioningStatus}
                />
                {/* ----------- Pagination ----------- */}
                <PaginationContainer>
                    <Pagination
                        current={page}
                        pageSize={pageSize}
                        total={total}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </PaginationContainer>
            </ExecutorsContainer>
            {showCreatePoolModal && (
                <CreateRemoteExecutorPoolModal
                    visible={showCreatePoolModal}
                    onCancel={() => setShowCreatePoolModal(false)}
                    onSuccessfulCreate={() => setTimeout(onRefresh, 500)}
                />
            )}
            {showPoolProvisioningStatusForUrn && (
                <RemoteExecutorPoolProvisioningPreviewModal
                    visible
                    onClose={() => setShowPoolProvisioningStatusForUrn(null)}
                    getPool={() => refetchPoolProvisioningStatus()}
                    pool={poolProvisioningStatus}
                />
            )}
        </>
    );
};
