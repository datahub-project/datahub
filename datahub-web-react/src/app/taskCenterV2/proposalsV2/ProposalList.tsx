import { Typography, message } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import ActionsBar from '@app/taskCenterV2/proposalsV2/ActionsBar';
import { ProposalsEntitySelect } from '@app/taskCenterV2/proposalsV2/ProposalsEntitySelect';
import ProposalsTable from '@app/taskCenterV2/proposalsV2/proposalsTable/ProposalsTable';
import useGetActionRequestsQueryInputs from '@app/taskCenterV2/proposalsV2/useGetActionRequestsQueryInputs';
import useGetEntitySuggestions from '@app/taskCenterV2/proposalsV2/useGetEntitySuggestions';
import {
    ACTION_REQUEST_DEFAULT_FACETS,
    ACTION_REQUEST_DISPLAY_FACETS,
    PROPOSALS_FILTER_LABELS,
} from '@app/taskCenterV2/utils/constants';
import { Pagination } from '@src/alchemy-components';
import FilterSection from '@src/app/sharedV2/filters/FilterSection';
import usePagination from '@src/app/sharedV2/pagination/usePagination';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { useListActionRequestsQuery } from '@graphql/actionRequest.generated';
import { ActionRequest, ActionRequestAssignee, EntityType, FacetFilterInput, FilterOperator } from '@types';

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
    justify-content: space-between;
    padding: 2px;
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
}: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [selectedUrns, setSelectedUrns] = useState<string[]>([]);
    const { start, pageSize, setPageSize, page, setPage } = usePagination(DEFAULT_PAGE_SIZE);

    const { filters, orFilters, onChangeFilters } = useGetActionRequestsQueryInputs({
        useUrlParams,
        defaultFilters,
        initialFilters,
    });

    const entitiesSelected = filters?.find((f) => f.field === 'resource')?.values || [];
    const { loading, error, data, refetch } = useListActionRequestsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
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
        data?.listActionRequests?.facets?.filter((facet) =>
            (filterFacets || ACTION_REQUEST_DISPLAY_FACETS).includes(facet?.field || ''),
        ) || [];

    const { suggestions: entitySuggestions, loading: entitySuggestionsLoading } = useGetEntitySuggestions({
        activeFilters: filters,
        facets: data?.listActionRequests?.facets || [],
    });

    const actionRequests = useMemo(() => data?.listActionRequests?.actionRequests || [], [data]);
    const totalActionRequests = data?.listActionRequests?.total || 0;
    const hasPagination = totalActionRequests > pageSize;

    const onActionRequestUpdate = () => {
        refetch();
    };

    const FinalContainer = isShowNavBarRedesign ? Container : React.Fragment;

    const onSelectEntities = (selectedValues: string[]) => {
        const entityFilter: FacetFilterInput = {
            condition: FilterOperator.Equal,
            field: 'resource',
            negated: false,
            values: selectedValues,
        };
        onChangeFilters([entityFilter], true);
    };

    const handleFiltersChange = (newFilters: FacetFilterInput[]): void => {
        // clear the table selection upon filters change
        setSelectedUrns([]);
        setPage(1);
        onChangeFilters(newFilters);
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
                            defaultSuggestionsLoading={entitySuggestionsLoading}
                            defaultSuggestions={entitySuggestions}
                            selected={entitiesSelected}
                            onUpdate={onSelectEntities}
                        />
                        <FilterSection
                            name="proposals"
                            loading={loading}
                            availableFilters={facets || []}
                            activeFilters={filters}
                            onChangeFilters={handleFiltersChange}
                            customFilterLabels={PROPOSALS_FILTER_LABELS}
                            aggregationsEntityTypes={[EntityType.ActionRequest]}
                            noOfLoadingSkeletons={1}
                        />
                    </ProposalsTableHeader>
                )}
                <ProposalsTable
                    onRowClick={onProposalClick}
                    actionRequests={actionRequests as ActionRequest[]}
                    isLoading={loading}
                    enableSelection={enableSelection}
                    isRowSelectionDisabled={(record: ActionRequest) => {
                        return record.status === 'COMPLETED';
                    }}
                    onActionRequestUpdate={onActionRequestUpdate}
                    selectedKeys={selectedUrns}
                    setSelectedKeys={setSelectedUrns}
                    showPendingView={showPendingView}
                />
                <FooterContainer>
                    {selectedUrns.length > 0 && (
                        <ActionsBar
                            selectedUrns={selectedUrns}
                            setSelectedUrns={setSelectedUrns}
                            onActionRequestUpdate={onActionRequestUpdate}
                            hasPagination={hasPagination}
                        />
                    )}
                    <Pagination
                        currentPage={page}
                        itemsPerPage={pageSize}
                        totalPages={totalActionRequests}
                        onPageChange={(newPage) => setPage(newPage)}
                        showSizeChanger
                        onShowSizeChange={(_currNum, newNum) => setPageSize(newNum)}
                        loading={loading}
                        hideOnSinglePage
                        showLessItems
                    />
                </FooterContainer>
            </ActionRequestsContainer>
        </FinalContainer>
    );
};
