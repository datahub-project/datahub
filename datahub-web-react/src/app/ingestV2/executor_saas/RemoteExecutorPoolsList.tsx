// SaaS only
import { Button, SearchBar } from '@components';
import { Pagination } from 'antd';
import { ArrowClockwise } from 'phosphor-react';
import * as QueryString from 'query-string';
import React, { useEffect, useRef, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { INGESTION_TAB_QUERY_PARAMS } from '@app/ingestV2/constants';
import CreateRemoteExecutorPoolModal from '@app/ingestV2/executor_saas/CreateRemoteExecutorPoolModal';
import { RemoteExecutorPoolProvisioningPreviewModal } from '@app/ingestV2/executor_saas/RemoteExecutorPoolProvisioningPreviewModal';
import { RemoteExecutorPoolsTable } from '@app/ingestV2/executor_saas/RemoteExecutorPoolsTable';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
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
const POLL_INTERVAL = 10000;
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
    padding: 0 20px 16px 0;
    height: auto;
    height: auto;
    box-shadow: none;
    border-bottom: none;
`;

const StyledSearchBar = styled(SearchBar)`
    width: 400px;
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
    selectedTab?: TabType | null;
    showCreatePoolModal: boolean;
    setShowCreatePoolModal: (show: boolean) => void;
    shouldPreserveParams: React.MutableRefObject<boolean>;
};

export const RemoteExecutorPoolsList = ({
    selectedTab,
    showCreatePoolModal,
    setShowCreatePoolModal,
    shouldPreserveParams,
}: Props) => {
    const defaultQuery = useQueryParamValue('pool');

    // ---------------------- load & search data ---------------------- //
    const [query, setQuery] = useState<string | undefined>(typeof defaultQuery === 'string' ? defaultQuery : undefined);
    const [page, setPage] = useState(1);
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    useEffect(() => {
        if (typeof defaultQuery === 'string' && defaultQuery?.length) {
            setQuery(defaultQuery);
        }
    }, [defaultQuery]);

    const { loading, error, data, refetch, startPolling, stopPolling } = useListRemoteExecutorPoolsQuery({
        variables: {
            start,
            count: pageSize,
            query,
        },
        fetchPolicy: 'no-cache',
    });
    const total = data?.listRemoteExecutorPools?.total;
    const remoteExecutorPools = (data?.listRemoteExecutorPools?.remoteExecutorPools ?? []) as RemoteExecutorPool[];

    useEffect(() => {
        if (selectedTab === TabType.RemoteExecutors) {
            startPolling(POLL_INTERVAL);
        } else {
            stopPolling();
        }
        return () => stopPolling();
    }, [selectedTab, startPolling, stopPolling]);

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
    const onViewSourcesForPool = (pool: string) => {
        const preserveParams = shouldPreserveParams;
        preserveParams.current = true;
        // first, add pool to query parameters
        const newParams = { [INGESTION_TAB_QUERY_PARAMS.pool]: pool };
        history.replace({
            pathname: tabUrlMap[TabType.Sources],
            search: QueryString.stringify(newParams),
        });
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
