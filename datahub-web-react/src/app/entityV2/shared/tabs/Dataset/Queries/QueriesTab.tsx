import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';
import styled from 'styled-components/macro';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import EmptyQueriesSection from '@app/entityV2/shared/tabs/Dataset/Queries/EmptyQueriesSection';
import QueriesListSection from '@app/entityV2/shared/tabs/Dataset/Queries/QueriesListSection';
import QueryBuilderModal from '@app/entityV2/shared/tabs/Dataset/Queries/QueryBuilderModal';
import {
    addQueryToListQueriesCache,
    removeQueryFromListQueriesCache,
    updateListQueriesCache,
} from '@app/entityV2/shared/tabs/Dataset/Queries/cacheUtils';
import { QueriesTabSection } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import useDownstreamQueries from '@app/entityV2/shared/tabs/Dataset/Queries/useDownstreamQueries';
import { useHighlightedQueries } from '@app/entityV2/shared/tabs/Dataset/Queries/useHighlightedQueries';
import { usePopularQueries } from '@app/entityV2/shared/tabs/Dataset/Queries/usePopularQueries';
import { useRecentQueries } from '@app/entityV2/shared/tabs/Dataset/Queries/useRecentQueries';
import Loading from '@app/shared/Loading';
import usePrevious from '@app/shared/usePrevious';

import { GetDatasetQuery } from '@graphql/dataset.generated';

const Content = styled.div<{ $backgroundColor: string }>`
    height: 100%;
    overflow: auto;
    display: flex;
    flex-direction: column;
    gap: 8px;
    background-color: ${(props) => props.$backgroundColor};
`;

export default function QueriesTab() {
    const { t } = useTranslation('entity.profile.queries');
    const theme = useTheme();
    const isSeparateSiblings = useIsSeparateSiblingsMode();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const entityUrn = baseEntity?.dataset?.urn;
    const canViewQueries = baseEntity?.dataset?.privileges?.canViewQueries ?? false;
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
    } = useHighlightedQueries({ entityUrn, siblingUrn, filterText, canViewQueries });

    /**
     * Fetch the List of Popular Queries
     */
    const { selectedUsersFilter, setSelectedUsersFilter, selectedColumnsFilter, setSelectedColumnsFilter } =
        usePopularQueries({ entityUrn, siblingUrn, filterText, canViewQueries });

    /**
     * Fetch the List of Downstream Queries
     */
    const { downstreamQueries, loading: downstreamQueriesLoading } = useDownstreamQueries(filterText, canViewQueries);

    /**
     * Fetch the List of Recent (auto-extracted) Queries
     */
    const { recentQueries, loading: recentQueriesLoading } = useRecentQueries({
        entityUrn,
        siblingUrn,
        filterText,
        canViewQueries,
    });

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
    const isLoading = !entityUrn || highlightedQueriesLoading || downstreamQueriesLoading || recentQueriesLoading;
    const showEmptyView =
        !isLoading && !recentQueries.length && !highlightedQueries.length && !downstreamQueries.length;

    const showHighlightedSection = highlightedQueries.length > 0 || highlightedQueriesLoading;
    const showDownstreamSection = downstreamQueries.length > 0;
    const showRecentSection = recentQueries.length > 0;
    const visibleSections = [
        { section: QueriesTabSection.Highlighted, isVisible: showHighlightedSection },
        { section: QueriesTabSection.Downstream, isVisible: showDownstreamSection },
        { section: QueriesTabSection.Recent, isVisible: showRecentSection },
    ].filter(({ isVisible }) => isVisible);
    const fillHeightSection = visibleSections[visibleSections.length - 1]?.section;

    // shared props with all of the QueriesListSection components below
    const props = {
        showDetails: false,
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
            <Content $backgroundColor={showLoading || showEmptyView ? theme.colors.bg : theme.colors.bgSurface}>
                {showLoading && <Loading />}
                {!showLoading && !canViewQueries && (
                    <EmptyQueriesSection
                        sectionName={t('queriesTab.queriesTitle')}
                        emptyText={t('queriesTab.noViewPermission')}
                        showButton={false}
                    />
                )}
                {!showLoading && canViewQueries && (
                    <>
                        {showHighlightedSection && (
                            <QueriesListSection
                                title={t('queriesTab.highlightedQueriesTitle')}
                                section={QueriesTabSection.Highlighted}
                                tooltip={t('queriesTab.highlightedQueriesTooltip')}
                                tooltipPosition="bottom"
                                queries={highlightedQueries}
                                loading={highlightedQueriesLoading}
                                totalQueries={highlightedTotal}
                                pagination={highlightedPagination}
                                sorting={highlightedSorting}
                                addQueryDisabled={!canEditQueries}
                                onAddQuery={() => setShowQueryBuilder(true)}
                                isTopSection
                                fillHeight={fillHeightSection === QueriesTabSection.Highlighted}
                                {...props}
                            />
                        )}
                        {!showHighlightedSection && (
                            <EmptyQueriesSection
                                sectionName={t('queriesTab.highlightedQueriesTitle')}
                                tooltip={t('queriesTab.highlightedQueriesTooltip')}
                                tooltipPosition="bottom"
                                showButton
                                buttonLabel={t('queriesTab.addHighlightedQueryButton')}
                                isButtonDisabled={!canEditQueries}
                                onButtonClick={() => setShowQueryBuilder(true)}
                            />
                        )}
                        {showDownstreamSection && (
                            <QueriesListSection
                                title={t('queriesTab.downstreamQueriesTitle')}
                                section={QueriesTabSection.Downstream}
                                tooltip={t('queriesTab.downstreamQueriesTooltip')}
                                queries={downstreamQueries}
                                totalQueries={downstreamQueries.length}
                                fillHeight={fillHeightSection === QueriesTabSection.Downstream}
                                {...props}
                            />
                        )}
                        {showRecentSection && (
                            <QueriesListSection
                                title={t('queriesTab.recentQueriesTitle')}
                                section={QueriesTabSection.Recent}
                                tooltip={t('queriesTab.recentQueriesTooltip')}
                                queries={recentQueries}
                                totalQueries={recentQueries.length}
                                fillHeight={fillHeightSection === QueriesTabSection.Recent}
                                {...props}
                            />
                        )}
                    </>
                )}
            </Content>
            {showQueryBuilder && (
                <QueryBuilderModal
                    datasetUrn={baseEntity?.dataset?.urn}
                    onClose={() => setShowQueryBuilder(false)}
                    onSubmit={onQueryCreated}
                />
            )}
        </>
    );
}
