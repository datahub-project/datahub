// SaaS only
import TabToolbar from '@src/app/entityV2/shared/components/styled/TabToolbar';
import { Message } from '@src/app/shared/Message';
import * as QueryString from 'query-string';
import {
    useListRemoteExecutorPoolsQuery,
    useUpdateDefaultRemoteExecutorPoolMutation,
} from '@src/graphql/remote_executor.saas.generated';
import { Button, Pagination } from 'antd';
import { ArrowClockwise, Plus } from 'phosphor-react';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';
import { RemoteExecutorPool } from '@src/types.generated';
import { ONE_SECOND_IN_MS } from '@src/app/entity/shared/tabs/Dataset/Queries/utils/constants';
import { debounce } from 'lodash';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { SearchBar } from '@src/app/search/SearchBar';
import { useQueryParamValue } from '@src/app/entityV2/shared/useQueryParamValue';
import { useUserContext } from '@src/app/context/useUserContext';
import { useHistory } from 'react-router';
import { INGESTION_TAB_QUERY_PARAMS } from '../constants';
import { TabType } from '../types';
import { RemoteExecutorPoolsTable } from './RemoteExecutorPoolsTable';
import CreateRemoteExecutorPoolModal from './CreateRemoteExecutorPoolModal';

const DEFAULT_PAGE_SIZE = 25;
const REMOTE_EXECUTORS_CREATE_SOURCE_ID = 'REMOTE_EXECUTORS_CREATE_SOURCE_ID';
const REMOTE_EXECUTORS_REFRESH_SOURCE_ID = 'REMOTE_EXECUTORS_REFRESH_SOURCE_ID';

// TODO: remove this once SQS provisioning is ready
const IS_CREATE_POOL_ENABLED = false;

const ExecutorsContainer = styled.div``;

const PlusStyled = styled(Plus)`
    position: relative;
    top: 2px;
`;
const ArrowClockwiseStyled = styled(ArrowClockwise)`
    position: relative;
    top: 2px;
`;

type Props = {
    onSwitchTab: (tab: string) => void;
};

export const RemoteExecutorPoolsList = ({ onSwitchTab }: Props) => {
    const me = useUserContext();
    const canManagePools = me.platformPrivileges?.manageIngestion;

    const entityRegistry = useEntityRegistry();
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

    const onSearch = debounce((newQuery: string | undefined) => {
        setQuery(newQuery);
        setPage(1);
    }, ONE_SECOND_IN_MS);

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
        // first, add pool to query parameters
        const newParams = { [INGESTION_TAB_QUERY_PARAMS.pool]: pool };
        const newSearch = QueryString.stringify(newParams);
        history.push(`${window.location.pathname}${newSearch ? `?${newSearch}` : ''}`);
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
                <TabToolbar>
                    <div>
                        {canManagePools && IS_CREATE_POOL_ENABLED && (
                            <Button id={REMOTE_EXECUTORS_CREATE_SOURCE_ID} type="text" onClick={onCreatePool}>
                                <PlusStyled />
                                <span style={{ marginLeft: 4 }}>Create</span>
                            </Button>
                        )}
                        <Button id={REMOTE_EXECUTORS_REFRESH_SOURCE_ID} type="text" onClick={onRefresh}>
                            <ArrowClockwiseStyled />
                            <span style={{ marginLeft: 4 }}>Refresh</span>
                        </Button>
                    </div>
                    <SearchBar
                        searchInputRef={searchInputRef}
                        initialQuery={query || ''}
                        placeholderText="Search pools..."
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
                        onQueryChange={onSearch}
                        entityRegistry={entityRegistry}
                        hideRecommendations
                    />
                </TabToolbar>
                {/* ----------- Table ----------- */}
                <RemoteExecutorPoolsTable
                    pools={remoteExecutorPools}
                    onRefresh={onRefresh}
                    updateDefaultPool={updateDefaultPool}
                    viewSourcesForPool={onViewSourcesForPool}
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
