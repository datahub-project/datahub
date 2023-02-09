import { Button, Input, Pagination, Typography } from 'antd';
import { PlusOutlined, SearchOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import React, { useState } from 'react';
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
`;

const MAX_QUERIES_COUNT = 1000;
const DEFAULT_PAGE_SIZE = 10;
const DEFAULT_MAX_RECENT_QUERIES = 10;

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

export default function QueriesTab() {
    const appConfig = useAppConfig();
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const [showQueryBuilder, setShowQueryBuilder] = useState(false);

    const [filterText, setFilterText] = useState('');

    /**
     * Pagination State
     */
    const [page, setPage] = useState(0);
    const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);

    const { data: highlightedQueriesData, refetch } = useListQueriesQuery({
        variables: { input: { datasetUrn: baseEntity?.dataset?.urn, start: 0, count: MAX_QUERIES_COUNT } },
        skip: !baseEntity?.dataset?.urn,
    });

    const { data: recentQueriesData } = useGetRecentQueriesQuery({
        variables: { urn: baseEntity?.dataset?.urn as string },
        skip: !baseEntity?.dataset?.urn,
    });

    const highlightedQueries = filterQueries(
        filterText,
        (highlightedQueriesData?.listQueries?.queries as QueryEntity[]) || [],
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

    const onQueryCreated = () => {
        // Use cache
        setTimeout(() => refetch(), 3000);
        setShowQueryBuilder(false);
    };

    const onQueryDeleted = () => {
        setTimeout(() => refetch(), 3000);
    };

    const onQueryEdited = () => {
        setTimeout(() => refetch(), 3000);
    };

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
            <QueriesContent>
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
                            {totalQueries > pageSize && (
                                <Pagination
                                    current={page}
                                    pageSize={pageSize}
                                    total={totalQueries}
                                    showLessItems
                                    onChange={setPage}
                                    onShowSizeChange={(_currSize, newSize) => setPageSize(newSize)}
                                    showSizeChanger={false}
                                />
                            )}
                        </QueriesSection>
                    </>
                )}
                {recentQueries.length > 0 && (
                    <>
                        <QueriesTitle level={4}>Recently Executed</QueriesTitle>
                        <QueriesSection>
                            {recentQueries.map((query) => (
                                <Query query={query.query} executedAtMs={query.dateMs} />
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
