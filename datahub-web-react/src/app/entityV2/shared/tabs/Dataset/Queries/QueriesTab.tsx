import styled from 'styled-components';
import React, { useState } from 'react';
import { debounce } from 'lodash';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../../../entity/shared/EntityContext';
import QueryBuilderModal from './QueryBuilderModal';
import EmptyQueries from './EmptyQueries';
import { addQueryToListQueriesCache, removeQueryFromListQueriesCache, updateListQueriesCache } from './cacheUtils';
import { HALF_SECOND_IN_MS, MAX_QUERIES_COUNT, MAX_ROWS_BEFORE_DEBOUNCE } from './utils/constants';
import QueriesTabToolbar from './QueriesTabToolbar';
import QueriesListSection from './QueriesListSection';
import useDownstreamQueries from './useDownstreamQueries';
import { QueriesTabSection } from './types';
import { useHighlightedQueries } from './useHighlightedQueries';
import { usePopularQueries } from './usePopularQueries';
import { useRecentQueries } from './useRecentQueries';
import Loading from '../../../../../shared/Loading';

const Content = styled.div`
    padding: 24px;
    height: 100%;
    overflow: auto;
    display: flex;
    flex-direction: column;
    gap: 24px;
`;

export default function QueriesTab() {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const entityUrn = baseEntity?.dataset?.urn;
    const canEditQueries = baseEntity?.dataset?.privileges?.canEditQueries || false;
    const siblingUrn = baseEntity?.dataset?.siblings?.siblings?.[0]?.urn;

    const [showQueryBuilder, setShowQueryBuilder] = useState(false);
    const [filterText, setFilterText] = useState('');

    /**
     * Fetch the List of Custom (Highlighted) Queries
     */
    const {
        highlightedQueries,
        client,
        loading: highlightedQueriesLoading,
    } = useHighlightedQueries({ entityUrn, siblingUrn, filterText });

    /**
     * Fetch the List of Popular Queries
     */
    const { popularQueries, loading: popularQueriesLoading } = usePopularQueries({ entityUrn, siblingUrn, filterText });

    /**
     * Fetch the List of Downstream Queries
     */
    const { downstreamQueries, loading: downstreamQueriesLoading } = useDownstreamQueries(filterText);

    /**
     * Fetch the List of Recent (auto-extracted) Queries
     */
    const { recentQueries, loading: recentQueriesLoading } = useRecentQueries({ entityUrn, siblingUrn, filterText });

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

    const isLoading =
        !entityUrn ||
        highlightedQueriesLoading ||
        popularQueriesLoading ||
        downstreamQueriesLoading ||
        recentQueriesLoading;
    const showEmptyView =
        !isLoading &&
        !recentQueries.length &&
        !highlightedQueries.length &&
        !downstreamQueries.length &&
        !popularQueries.length;

    // shared props with all of the QueriesListSection components below
    const props = {
        showDetails: false,
        showDelete: false,
        showEdit: false,
        onDeleted: onQueryDeleted,
        onEdited: onQueryEdited,
    };

    return (
        <>
            <QueriesTabToolbar
                addQueryDisabled={!canEditQueries}
                onAddQuery={() => setShowQueryBuilder(true)}
                onChangeSearch={debouncedSetFilterText}
            />
            <Content>
                {isLoading && <Loading />}
                {!isLoading && (
                    <>
                        {showEmptyView && (
                            <EmptyQueries
                                readOnly={!canEditQueries}
                                onClickAddQuery={() => setShowQueryBuilder(true)}
                            />
                        )}
                        {highlightedQueries.length > 0 && (
                            <QueriesListSection
                                title="Highlighted Queries"
                                section={QueriesTabSection.Highlighted}
                                tooltip="Curated queries relevant to this dataset"
                                tooltipPosition="bottom"
                                queries={highlightedQueries}
                                {...props}
                            />
                        )}
                        {popularQueries.length > 0 && (
                            <QueriesListSection
                                title="Popular Queries"
                                section={QueriesTabSection.Popular}
                                tooltip="The most popular queries that were run against this dataset"
                                queries={popularQueries}
                                {...props}
                            />
                        )}
                        {downstreamQueries.length > 0 && (
                            <QueriesListSection
                                title="Downstream Queries"
                                section={QueriesTabSection.Downstream}
                                tooltip="Queries that power downstream assets"
                                queries={downstreamQueries}
                                {...props}
                            />
                        )}
                        {recentQueries.length > 0 && (
                            <QueriesListSection
                                title="Recent Queries"
                                section={QueriesTabSection.Recent}
                                tooltip="Recently executed queries against this dataset"
                                queries={recentQueries}
                                {...props}
                            />
                        )}
                    </>
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
