import styled from 'styled-components';
import React, { useState } from 'react';
import { debounce } from 'lodash';
import { useListQueriesQuery } from '../../../../../../graphql/query.generated';
import { GetDatasetQuery, useGetRecentQueriesQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../EntityContext';
import getTopNQueries from './utils/getTopNQueries';
import { useAppConfig } from '../../../../../useAppConfig';
import QueryBuilderModal from './QueryBuilderModal';
import EmptyQueries from './EmptyQueries';
import { addQueryToListQueriesCache, removeQueryFromListQueriesCache, updateListQueriesCache } from './cacheUtils';
import {
    DEFAULT_MAX_RECENT_QUERIES,
    HALF_SECOND_IN_MS,
    MAX_QUERIES_COUNT,
    MAX_ROWS_BEFORE_DEBOUNCE,
} from './utils/constants';
import { filterQueries } from './utils/filterQueries';
import QueriesTabToolbar from './QueriesTabToolbar';
import QueriesListSection from './QueriesListSection';

const Content = styled.div`
    padding: 24px;
    height: 100%;
    overflow: scroll;
`;

export default function QueriesTab() {
    const appConfig = useAppConfig();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const canEditQueries = baseEntity?.dataset?.privileges?.canEditQueries || false;

    const [showQueryBuilder, setShowQueryBuilder] = useState(false);
    const [filterText, setFilterText] = useState('');

    /**
     * Fetch the List of Custom (Highlighted) Queries
     */
    const { data: highlightedQueriesData, client } = useListQueriesQuery({
        variables: { input: { datasetUrn: baseEntity?.dataset?.urn, start: 0, count: MAX_QUERIES_COUNT } },
        skip: !baseEntity?.dataset?.urn,
        fetchPolicy: 'cache-first',
    });

    const highlightedQueries = filterQueries(
        filterText,
        (highlightedQueriesData?.listQueries?.queries || []).map((queryEntity) => ({
            urn: queryEntity.urn,
            title: queryEntity.properties?.name || undefined,
            description: queryEntity.properties?.description || undefined,
            query: queryEntity.properties?.statement?.value || '',
            createdTime: queryEntity?.properties?.created?.time,
        })),
    );

    /**
     * Fetch the List of Recent (auto-extracted) Queries
     */
    const { data: recentQueriesData } = useGetRecentQueriesQuery({
        variables: { urn: baseEntity?.dataset?.urn as string },
        skip: !baseEntity?.dataset?.urn,
        fetchPolicy: 'cache-first',
    });

    const recentQueries = filterQueries(
        filterText,
        (
            getTopNQueries(
                appConfig?.config?.visualConfig?.queriesTab?.queriesTabResultSize || DEFAULT_MAX_RECENT_QUERIES,
                recentQueriesData?.dataset?.usageStats?.buckets,
            ) || []
        ).map((recentQuery) => ({ query: recentQuery.query })),
    );

    const debouncedSetFilterText = debounce(
        (e: React.ChangeEvent<HTMLInputElement>) => setFilterText(e.target.value),
        highlightedQueries.length > MAX_ROWS_BEFORE_DEBOUNCE ? HALF_SECOND_IN_MS : 0,
    );

    const onQueryCreated = (newQuery) => {
        addQueryToListQueriesCache(newQuery, client, MAX_QUERIES_COUNT, baseEntity?.dataset?.urn);
        setShowQueryBuilder(false);
    };

    const onQueryDeleted = (query) => {
        removeQueryFromListQueriesCache(query.urn, client, 1, MAX_QUERIES_COUNT, baseEntity?.dataset?.urn);
    };

    const onQueryEdited = (query) => {
        updateListQueriesCache(query.urn, query, client, 1, MAX_QUERIES_COUNT, baseEntity?.dataset?.urn);
    };

    const showEmptyView = !recentQueries.length && !highlightedQueries.length;

    return (
        <>
            <QueriesTabToolbar
                addQueryDisabled={!canEditQueries}
                onAddQuery={() => setShowQueryBuilder(true)}
                onChangeSearch={debouncedSetFilterText}
            />
            <Content>
                {showEmptyView && (
                    <EmptyQueries readOnly={!canEditQueries} onClickAddQuery={() => setShowQueryBuilder(true)} />
                )}
                {highlightedQueries.length > 0 && (
                    <QueriesListSection
                        title="Highlighted Queries"
                        tooltip="Shared queries relevant to this dataset"
                        queries={highlightedQueries}
                        showEdit
                        showDelete
                        onDeleted={onQueryDeleted}
                        onEdited={onQueryEdited}
                    />
                )}
                {recentQueries.length > 0 && (
                    <QueriesListSection
                        title="Recent Queries"
                        tooltip="Queries that have been recently run against this dataset"
                        queries={recentQueries}
                        showDetails={false}
                        showDelete={false}
                        showEdit={false}
                    />
                )}
            </Content>
            {showQueryBuilder && (
                <QueryBuilderModal
                    datasetUrn={baseEntity.dataset?.urn}
                    onClose={() => setShowQueryBuilder(false)}
                    onSubmit={onQueryCreated}
                />
            )}
        </>
    );
}
