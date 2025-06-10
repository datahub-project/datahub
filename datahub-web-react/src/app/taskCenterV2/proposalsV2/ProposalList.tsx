import { NetworkStatus } from '@apollo/client';
import { Typography, message } from 'antd';
import { isEqual, sortBy } from 'lodash';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { combineOrFilters } from '@app/searchV2/utils/filterUtils';
import ActionsBar from '@app/taskCenterV2/proposalsV2/ActionsBar';
import { ProposalsEntitySelect } from '@app/taskCenterV2/proposalsV2/ProposalsEntitySelect';
import ProposalsTable from '@app/taskCenterV2/proposalsV2/proposalsTable/ProposalsTable';
import useGetActionRequestsQueryInputs from '@app/taskCenterV2/proposalsV2/useGetActionRequestsQueryInputs';
import useGetEntitySuggestions from '@app/taskCenterV2/proposalsV2/useGetEntitySuggestions';
import { isFilteringForPendingProposals } from '@app/taskCenterV2/proposalsV2/utils';
import {
    ACTION_REQUEST_DEFAULT_FACETS,
    ACTION_REQUEST_DISPLAY_FACETS,
    PROPOSALS_FILTER_LABELS,
} from '@app/taskCenterV2/utils/constants';
import { Pagination } from '@src/alchemy-components';
import FilterSection from '@src/app/sharedV2/filters/FilterSection';
import usePagination from '@src/app/sharedV2/pagination/usePagination';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { useAggregateActionRequestsQuery, useListActionRequestsV2Query } from '@graphql/actionRequest.generated';
import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { ActionRequest, ActionRequestAssignee, Entity, EntityType, FacetFilterInput, FilterOperator } from '@types';

const DEFAULT_PAGE_SIZE = 25;

const ActionRequestsContainer = styled.div<{ $isShowNavBarRedesign?: boolean; $height?: string }>`
    overflow: hidden;
    flex: 1;
    display: flex;
    flex-direction: column;
    // TODO: Test this in old UI
    ${(props) =>
        `
        height: ${props.$height ? props.$height : '100%'};
    `}
`;

const Container = styled.div`
    display: contents;
`;

const FooterContainer = styled.div`
    position: relative;
`;

const ActionRequestsTitle = styled(Typography.Title)`
    && {
        margin-bottom: 24px;
    }
`;

const ProposalsTableHeader = styled.div`
    display: flex;
    gap: 8px;
`;

type Props = {
    title?: string;
    assignee?: ActionRequestAssignee;
    onProposalClick?: (record: ActionRequest) => void;
    resourceUrn?: string;
    showFilters?: boolean;
    useUrlParams?: boolean;
    height?: string;
    defaultFilters?: FacetFilterInput[];
    initialFilters?: FacetFilterInput[];
    filterFacets?: Array<string>;
    getAllActionRequests?: boolean;
    showPendingView?: boolean;
    enableSelection?: boolean;
    showAssignee?: boolean;
};

export const ProposalList = ({
    title,
    assignee,
    onProposalClick,
    resourceUrn,
    showFilters = false,
    useUrlParams = false,
    height,
    defaultFilters = [],
    initialFilters = [],
    filterFacets,
    getAllActionRequests = false,
    showPendingView = false,
    enableSelection,
    showAssignee,
}: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [selectedUrns, setSelectedUrns] = useState<string[]>([]);
    const [selectedProposals, setSelectedProposals] = useState<ActionRequest[]>([]);
    const { start, pageSize, setPageSize, page, setPage } = usePagination(DEFAULT_PAGE_SIZE);
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [completedProposalUrns, setCompletedProposalUrns] = useState<string[]>([]);
    const { refetchUnfinishedTaskCount } = useUserContext();

    const { filters, orFilters, onChangeFilters } = useGetActionRequestsQueryInputs({
        useUrlParams,
        defaultFilters,
        initialFilters,
    });

    const entitiesSelected = filters?.find((f) => f.field === 'resource')?.values || [];
    const { data: selectedEntityData } = useGetEntitiesQuery({
        variables: { urns: entitiesSelected },
        fetchPolicy: 'cache-first',
    });

    const input = {
        start,
        count: pageSize,
        orFilters,
        assignee,
        resourceUrn,
        facets: ACTION_REQUEST_DEFAULT_FACETS,
        allActionRequests: getAllActionRequests,
    };
    const { loading, error, data, refetch, networkStatus } = useListActionRequestsV2Query({
        variables: {
            input: {
                start,
                count: pageSize,
                orFilters,
                assignee,
                resourceUrn,
                allActionRequests: getAllActionRequests,
            },
            includeAssignees: showAssignee,
        },
        fetchPolicy: 'cache-and-network',
        nextFetchPolicy: 'cache-first',
        notifyOnNetworkStatusChange: true,
    });
    const isLoading = loading && networkStatus !== NetworkStatus.refetch;

    const { loading: facetsLoading, data: facetsData } = useAggregateActionRequestsQuery({
        variables: {
            input: {
                count: 0,
                orFilters,
                assignee,
                resourceUrn,
                facets: ACTION_REQUEST_DEFAULT_FACETS,
                allActionRequests: getAllActionRequests,
            },
        },
        fetchPolicy: 'cache-and-network',
    });

    // Filtering the facets to be shown in the UI
    const facets =
        facetsData?.listActionRequests?.facets?.filter((facet) =>
            (filterFacets || ACTION_REQUEST_DISPLAY_FACETS).includes(facet?.field || ''),
        ) || [];

    const { suggestions: entitySuggestions, loading: entitySuggestionsLoading } = useGetEntitySuggestions({
        activeFilters: filters,
        facets: facetsData?.listActionRequests?.facets || [],
    });

    const actionRequests = useMemo(() => data?.listActionRequests?.actionRequests || [], [data]);
    const totalActionRequests = data?.listActionRequests?.total || 0;

    const [paginationTotal, setPaginationTotal] = useState(totalActionRequests);
    useEffect(() => {
        if (data?.listActionRequests?.actionRequests?.length) {
            setPaginationTotal(totalActionRequests);
        }
    }, [totalActionRequests, data]);

    const onActionRequestUpdate = (completedUrns: string[]) => {
        // if we're bulk accepting or rejecting, filter out the completed urns and show reload
        // this is necessary for elastic consistency issues with bulk accept/reject more than just a few
        if (completedUrns.length > 1) {
            setCompletedProposalUrns((existingUrns) => {
                const newCompletedUrns = [...existingUrns, ...completedUrns];
                const completedProposalUrnsFilter = [
                    { and: [{ field: 'urn', values: newCompletedUrns, negated: true }] },
                ];

                let finalOrFilters = orFilters;
                if (isFilteringForPendingProposals(orFilters)) {
                    finalOrFilters = combineOrFilters(finalOrFilters, completedProposalUrnsFilter);
                    refetch({ input: { ...input, orFilters: finalOrFilters } }); // new input causes loading
                }

                return newCompletedUrns;
            });
        } else {
            setSelectedUrns((prev) => prev.filter((u) => !completedUrns.includes(u)));
            // otherwise normal refetch will help reload the current page without showing loading
            setTimeout(() => {
                refetch();
            }, 5000);
        }

        // if we are selecting more than the pageSize, the pagination changes in the backend,
        // and it's better to redirect to page 1 here
        if (selectedUrns.length > pageSize) {
            setPage(1);
        }

        setTimeout(() => {
            refetchUnfinishedTaskCount();
        }, 3000);
    };

    const FinalContainer = isShowNavBarRedesign ? Container : React.Fragment;

    const onSelectEntities = (selectedValues: string[]) => {
        const entityFilter: FacetFilterInput = {
            condition: FilterOperator.Equal,
            field: 'resource',
            negated: false,
            values: selectedValues,
        };
        setPage(1);
        onChangeFilters([entityFilter], true);
    };

    const handleFiltersChange = (newFilters: FacetFilterInput[]): void => {
        const filtersChanged = !isEqual(
            // Sort both arrays by field to ensure consistent comparison order
            sortBy(filters, 'field'),
            sortBy(newFilters, 'field'),
        );

        // Set the page to 1 only when the filters have changed
        if (filtersChanged) {
            setPage(1);
            onChangeFilters(newFilters);
        }
    };

    // If there are no action requests when no filters are applied, the filter header shouldn't be shown
    const showFiltersHeader = showFilters && (filters?.length || !!totalActionRequests);

    return (
        <FinalContainer>
            {error && message.error('Failed to load proposals. An unknown error occurred!')}
            <ActionRequestsContainer $isShowNavBarRedesign={isShowNavBarRedesign} $height={height}>
                {title && <ActionRequestsTitle level={2}>{title}</ActionRequestsTitle>}
                {showFiltersHeader && (
                    <ProposalsTableHeader>
                        <ProposalsEntitySelect
                            defaultSuggestionsLoading={entitySuggestionsLoading && !entitySuggestions}
                            defaultSuggestions={entitySuggestions}
                            selected={entitiesSelected}
                            onUpdate={onSelectEntities}
                            fetchedSelectedEntityData={selectedEntityData?.entities as Entity[]}
                            loading={facetsLoading && !facetsData}
                        />
                        <FilterSection
                            name="proposals"
                            loading={facetsLoading && !facetsData}
                            availableFilters={facets || []}
                            activeFilters={filters}
                            onChangeFilters={handleFiltersChange}
                            customFilterLabels={PROPOSALS_FILTER_LABELS}
                            queryOptions={{
                                aggregationsEntityTypes: [EntityType.ActionRequest],
                                shouldApplyView: false,
                                includeAll: getAllActionRequests,
                            }}
                            noOfLoadingSkeletons={3}
                        />
                    </ProposalsTableHeader>
                )}
                <ProposalsTable
                    onRowClick={onProposalClick}
                    actionRequests={actionRequests as ActionRequest[]}
                    // Show loading indicator when there is no cached data, or the when the cached data is empty after bulk operations
                    isLoading={isLoading && (!data || data?.listActionRequests?.actionRequests?.length === 0)}
                    enableSelection={enableSelection}
                    isRowSelectionDisabled={(record: ActionRequest) => {
                        return record.status === 'COMPLETED';
                    }}
                    onActionRequestUpdate={onActionRequestUpdate}
                    selectedKeys={selectedUrns}
                    setSelectedKeys={setSelectedUrns}
                    setSelectedProposals={setSelectedProposals}
                    showPendingView={showPendingView}
                    showAssignee={showAssignee}
                />
                <FooterContainer>
                    {selectedUrns.length > 0 && (
                        <ActionsBar
                            selectedUrns={selectedUrns}
                            selectedProposals={selectedProposals}
                            setSelectedUrns={setSelectedUrns}
                            onActionRequestUpdate={onActionRequestUpdate}
                            hasPagination
                        />
                    )}
                    <Pagination
                        currentPage={page}
                        itemsPerPage={pageSize}
                        totalPages={paginationTotal}
                        onPageChange={(newPage) => setPage(newPage)}
                        showSizeChanger
                        onShowSizeChange={(_currNum, newNum) => {
                            setPageSize(newNum);
                            setPaginationTotal(totalActionRequests);
                        }}
                        hideOnSinglePage={!data}
                        showLessItems
                    />
                </FooterContainer>
            </ActionRequestsContainer>
        </FinalContainer>
    );
};
