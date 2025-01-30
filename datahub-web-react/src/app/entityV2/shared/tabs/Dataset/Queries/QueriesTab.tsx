import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import styled from 'styled-components/macro';
import React, { useEffect, useState } from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../../../entity/shared/EntityContext';
import QueryBuilderModal from './QueryBuilderModal';
import { addQueryToListQueriesCache, removeQueryFromListQueriesCache, updateListQueriesCache } from './cacheUtils';
import QueriesListSection from './QueriesListSection';
import useDownstreamQueries from './useDownstreamQueries';
import { QueriesTabSection } from './types';
import { useHighlightedQueries } from './useHighlightedQueries';
import { usePopularQueries } from './usePopularQueries';
import { useRecentQueries } from './useRecentQueries';
import Loading from '../../../../../shared/Loading';
import { useIsSeparateSiblingsMode } from '../../../../../entity/shared/siblingUtils';
import usePrevious from '../../../../../shared/usePrevious';
import EmptyQueriesSection from './EmptyQueriesSection';

const Content = styled.div<{ $backgroundColor: string }>`
    height: 100%;
    overflow: auto;
    display: flex;
    flex-direction: column;
    gap: 24px;
    background-color: ${(props) => props.$backgroundColor};
`;

export default function QueriesTab() {
    const isSeparateSiblings = useIsSeparateSiblingsMode();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const entityUrn = baseEntity?.dataset?.urn;
    const canEditQueries = baseEntity?.dataset?.privileges?.canEditQueries || false;
    const siblingUrn = isSeparateSiblings
        ? undefined
        : baseEntity?.dataset?.siblingsSearch?.searchResults?.[0]?.entity?.urn;

    const [showQueryBuilder, setShowQueryBuilder] = useState(false);
    // TODO: implement search filtering properly
    const [filterText] = useState('');
    const [hasLoadedInitially, setHasLoadedInitially] = useState(false);

    /**
     * Fetch the List of Custom (Highlighted) Queries
     */
    const {
        highlightedQueries,
        client,
        loading: highlightedQueriesLoading,
        pagination: highlightedPagination,
        total: highlightedTotal,
        sorting: highlightedSorting,
    } = useHighlightedQueries({ entityUrn, siblingUrn, filterText });

    /**
     * Fetch the List of Popular Queries
     */
    const {
        popularQueries,
        loading: popularQueriesLoading,
        pagination: popularQueriesPagination,
        total,
        sorting: popularSorting,
        selectedUsersFilter,
        setSelectedUsersFilter,
        selectedColumnsFilter,
        setSelectedColumnsFilter,
    } = usePopularQueries({ entityUrn, siblingUrn, filterText });

    /**
     * Fetch the List of Downstream Queries
     */
    const { downstreamQueries, loading: downstreamQueriesLoading } = useDownstreamQueries(filterText);

    /**
     * Fetch the List of Recent (auto-extracted) Queries
     */
    const { recentQueries, loading: recentQueriesLoading } = useRecentQueries({ entityUrn, siblingUrn, filterText });

    const onQueryCreated = (newQuery) => {
        addQueryToListQueriesCache(newQuery, client, highlightedPagination.count, entityUrn, siblingUrn);
        setShowQueryBuilder(false);
    };

    const onQueryDeleted = (query) => {
        removeQueryFromListQueriesCache(query.urn, client, 1, highlightedPagination.count, entityUrn, siblingUrn);
    };

    const onQueryEdited = (query) => {
        updateListQueriesCache(query.urn, query, client, 1, highlightedPagination.count, entityUrn, siblingUrn);
    };

    // can add something about initalLoading if there was never data, or have state that is like finishedInitialLoad = false, with useEffect
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
        selectedUsersFilter,
        setSelectedUsersFilter,
        selectedColumnsFilter,
        setSelectedColumnsFilter,
    };

    const previousIsLoading = usePrevious(isLoading);
    useEffect(() => {
        if (previousIsLoading && !isLoading && !hasLoadedInitially) {
            setHasLoadedInitially(true);
        }
    }, [previousIsLoading, isLoading, hasLoadedInitially]);

    const showLoading = isLoading && !hasLoadedInitially;

    return (
        <>
            <Content $backgroundColor={showLoading || showEmptyView ? 'white' : REDESIGN_COLORS.BACKGROUND}>
                {showLoading && <Loading />}
                {!showLoading && (
                    <>
                        {(highlightedQueries.length > 0 || highlightedQueriesLoading) && (
                            <QueriesListSection
                                title="Highlighted Queries"
                                section={QueriesTabSection.Highlighted}
                                tooltip="Curated queries relevant to this dataset"
                                tooltipPosition="bottom"
                                queries={highlightedQueries}
                                loading={highlightedQueriesLoading}
                                totalQueries={highlightedTotal}
                                pagination={highlightedPagination}
                                sorting={highlightedSorting}
                                addQueryDisabled={!canEditQueries}
                                onAddQuery={() => setShowQueryBuilder(true)}
                                isTopSection
                                {...props}
                            />
                        )}
                        {highlightedQueries.length === 0 && !highlightedQueriesLoading && (
                            <EmptyQueriesSection
                                sectionName="Highlighted Queries"
                                tooltip="Curated queries relevant to this dataset"
                                tooltipPosition="bottom"
                                showButton
                                buttonLabel="Add Highlighted Query"
                                isButtonDisabled={!canEditQueries}
                                onButtonClick={() => setShowQueryBuilder(true)}
                            />
                        )}
                        {(popularQueries.length > 0 || popularQueriesLoading) && (
                            <QueriesListSection
                                title="Popular Queries"
                                section={QueriesTabSection.Popular}
                                tooltip="The most popular queries that were run against this dataset"
                                queries={popularQueries}
                                loading={popularQueriesLoading}
                                totalQueries={total}
                                pagination={popularQueriesPagination}
                                sorting={popularSorting}
                                {...props}
                            />
                        )}
                        {downstreamQueries.length > 0 && (
                            <QueriesListSection
                                title="Downstream Queries"
                                section={QueriesTabSection.Downstream}
                                tooltip="Queries that power downstream assets"
                                queries={downstreamQueries}
                                totalQueries={downstreamQueries.length}
                                {...props}
                            />
                        )}
                        {recentQueries.length > 0 && (
                            <QueriesListSection
                                title="Recent Queries"
                                section={QueriesTabSection.Recent}
                                tooltip="Recently executed queries against this dataset"
                                queries={recentQueries}
                                totalQueries={recentQueries.length}
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
