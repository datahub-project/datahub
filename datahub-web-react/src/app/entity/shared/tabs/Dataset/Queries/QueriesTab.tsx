import { Button, Input, Pagination, Typography } from 'antd';
import { PlusOutlined, SearchOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import React, { useEffect, useRef, useState } from 'react';
import { debounce } from 'lodash';
import { useListQueriesQuery } from '../../../../../../graphql/query.generated';
import { GetDatasetQuery, useGetRecentQueriesQuery } from '../../../../../../graphql/dataset.generated';
import Query from './Query';
import { useBaseEntity } from '../../../EntityContext';
import getTopNQueries from './utils/getTopNQueries';
import { useAppConfig } from '../../../../../useAppConfig';
import TabToolbar from '../../../components/styled/TabToolbar';
import { QueryEntity } from '../../../../../../types.generated';
import QueryBuilderModal from './QueryBuilderModal';
import EmptyQueries from './EmptyQueries';
import { addQueryToListQueriesCache, removeQueryFromListQueriesCache, updateListQueriesCache } from './cacheUtils';

const QueriesSection = styled.div`
    margin-bottom: 28px;
    display: flex;
    align-items: center;
    justify-content: left;
    flex-wrap: wrap;
`;

const QueriesTitle = styled(Typography.Title)`
    && {
        margin-bottom: 20px;
    }
`;

const StyledInput = styled(Input)`
    border-radius: 70px;
    max-width: 300px;
`;

const QueriesContent = styled.div`
    padding: 24px;
    height: 100%;
    overflow: scroll;
`;

const StyledPagination = styled(Pagination)`
    padding: 0px 24px 24px 24px;
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const MAX_QUERIES_COUNT = 1000;
const DEFAULT_PAGE_SIZE = 6;
const DEFAULT_MAX_RECENT_QUERIES = 9;

const MAX_ROWS_BEFORE_DEBOUNCE = 50;
const HALF_SECOND_IN_MS = 500;

const filterQueries = (filterText, queries: QueryEntity[]) => {
    const lowerFilterText = filterText.toLowerCase();
    return queries.filter((query) => {
        return (
            query.properties?.name?.toLowerCase().includes(lowerFilterText) ||
            query.properties?.description?.toLowerCase().includes(lowerFilterText) ||
            query.properties?.statement?.value?.toLowerCase().includes(lowerFilterText)
        );
    });
};

const getCurrentPage = (queries: QueryEntity[], page: number, pageSize: number) => {
    const start = (page - 1) * pageSize;
    const end = start + pageSize;
    return queries.length >= end ? queries.slice(start, end) : queries.slice(start, queries.length);
};

export default function QueriesTab() {
    const appConfig = useAppConfig();
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const [showQueryBuilder, setShowQueryBuilder] = useState(false);

    const [filterText, setFilterText] = useState('');

    /**
     * Pagination State
     */
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);

    const { data: highlightedQueriesData, client } = useListQueriesQuery({
        variables: { input: { datasetUrn: baseEntity?.dataset?.urn, start: 0, count: MAX_QUERIES_COUNT } },
        skip: !baseEntity?.dataset?.urn,
        fetchPolicy: 'cache-first',
    });

    const { data: recentQueriesData } = useGetRecentQueriesQuery({
        variables: { urn: baseEntity?.dataset?.urn as string },
        skip: !baseEntity?.dataset?.urn,
        fetchPolicy: 'cache-first',
    });

    const highlightedQueries = getCurrentPage(
        filterQueries(filterText, (highlightedQueriesData?.listQueries?.queries as QueryEntity[]) || []),
        page,
        pageSize,
    );
    const totalQueries = highlightedQueriesData?.listQueries?.total || 0;

    const recentQueries =
        getTopNQueries(
            appConfig?.config?.visualConfig?.queriesTab?.queriesTabResultSize || DEFAULT_MAX_RECENT_QUERIES,
            recentQueriesData?.dataset?.usageStats?.buckets,
        ) || [];

    const debouncedSetFilterText = debounce(
        (e: React.ChangeEvent<HTMLInputElement>) => setFilterText(e.target.value),
        highlightedQueries.length > MAX_ROWS_BEFORE_DEBOUNCE ? HALF_SECOND_IN_MS : 0,
    );

    const onQueryCreated = (newQuery) => {
        addQueryToListQueriesCache(newQuery, client, MAX_QUERIES_COUNT, baseEntity?.dataset?.urn);
        setShowQueryBuilder(false);
    };

    const onQueryDeleted = (urn) => {
        removeQueryFromListQueriesCache(urn, client, 1, MAX_QUERIES_COUNT, baseEntity?.dataset?.urn);
    };

    const onQueryEdited = (newQuery) => {
        updateListQueriesCache(newQuery.urn, newQuery, client, 1, MAX_QUERIES_COUNT, baseEntity?.dataset?.urn);
    };

    const queriesContentRef = useRef(null);

    useEffect(() => {
        (queriesContentRef?.current as any)?.scrollIntoView({ behavior: 'smooth', block: 'start', inline: 'nearest' });
    }, [page]);

    const showEmptyView = !recentQueries.length && !highlightedQueries.length;

    return (
        <>
            <TabToolbar>
                <Button type="text" onClick={() => setShowQueryBuilder(true)}>
                    <PlusOutlined /> Add Query
                </Button>
                <StyledInput
                    placeholder="Search in queries..."
                    onChange={debouncedSetFilterText}
                    allowClear
                    prefix={<SearchOutlined />}
                />
            </TabToolbar>
            <QueriesContent ref={queriesContentRef}>
                {showEmptyView && <EmptyQueries onClickAddQuery={() => setShowQueryBuilder(true)} />}
                {highlightedQueries.length > 0 && (
                    <>
                        <QueriesTitle level={4}>Highlighted Queries</QueriesTitle>
                        <QueriesSection>
                            {highlightedQueries.map((query) => (
                                <Query
                                    urn={query.urn}
                                    title={query.properties?.name || undefined}
                                    description={query.properties?.description || undefined}
                                    query={query.properties?.statement?.value}
                                    createdAtMs={query.properties?.created?.time}
                                    showDelete
                                    showEdit
                                    onDeleted={onQueryDeleted}
                                    onEdited={onQueryEdited}
                                    filterText={filterText}
                                />
                            ))}
                        </QueriesSection>
                        {totalQueries > pageSize && (
                            <StyledPagination
                                current={page}
                                pageSize={pageSize}
                                total={totalQueries}
                                showLessItems
                                onChange={(newPage) => {
                                    setPage(newPage);
                                }}
                                onShowSizeChange={(_currSize, newSize) => setPageSize(newSize)}
                                showSizeChanger={false}
                            />
                        )}
                    </>
                )}
                {recentQueries.length > 0 && (
                    <>
                        <QueriesTitle level={4}>Recently Executed</QueriesTitle>
                        <QueriesSection>
                            {recentQueries.map((query) => (
                                <Query query={query.query} showDetails={false} />
                            ))}
                        </QueriesSection>
                    </>
                )}
            </QueriesContent>
            {baseEntity?.dataset?.urn && showQueryBuilder && (
                <QueryBuilderModal
                    datasetUrn={baseEntity.dataset?.urn}
                    onClose={() => setShowQueryBuilder(false)}
                    onSubmit={onQueryCreated}
                />
            )}
        </>
    );
}
