import React, { useMemo, useState } from 'react';
import { message, Typography } from 'antd';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { Pagination } from '@src/alchemy-components';
import FilterSection from '@src/app/sharedV2/filters/FilterSection';
import usePagination from '@src/app/sharedV2/pagination/usePagination';
import ProposalsTable from './proposalsTable/ProposalsTable';
import {
    ActionRequest,
    ActionRequestAssignee,
    EntityType,
    FacetFilterInput,
    FilterOperator,
} from '../../../types.generated';
import { useListActionRequestsQuery } from '../../../graphql/actionRequest.generated';
import ActionsBar from './ActionsBar';
import { ACTION_REQUEST_DEFAULT_FACETS, PROPOSALS_FILTER_LABELS } from '../utils/constants';
import useGetActionRequestsQueryInputs from './useGetActionRequestsQueryInputs';

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
`;

type Props = {
    title?: string;
    assignee?: ActionRequestAssignee;
    onProposalClick?: (record: ActionRequest) => void;
    resourceUrn?: string;
    showFilters?: boolean;
    useUrlParams?: boolean;
    height?: string;
    createdBy?: string;
    filterFacets?: Array<string>;
    getAllActionRequests?: boolean;
    showPendingView?: boolean;
};

export const ProposalList = ({
    title,
    assignee,
    onProposalClick,
    resourceUrn,
    showFilters = false,
    useUrlParams = false,
    height,
    createdBy,
    filterFacets,
    getAllActionRequests = false,
    showPendingView = false,
}: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [selectedUrns, setSelectedUrns] = useState<string[]>([]);
    const { start, pageSize, setPageSize, page, setPage } = usePagination(DEFAULT_PAGE_SIZE);
    const defaultFilters: Array<FacetFilterInput> = useMemo(
        () =>
            createdBy
                ? [
                      {
                          field: 'createdBy',
                          condition: FilterOperator.Equal,
                          values: [createdBy],
                          negated: false,
                      },
                  ]
                : [],
        [createdBy],
    );
    const { filters, orFilters, onChangeFilters } = useGetActionRequestsQueryInputs({ useUrlParams, defaultFilters });

    const { loading, error, data, refetch } = useListActionRequestsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                orFilters,
                assignee,
                resourceUrn,
                facets: filterFacets || ACTION_REQUEST_DEFAULT_FACETS,
                allActionRequests: getAllActionRequests,
            },
        },
        fetchPolicy: 'cache-and-network',
    });

    // Filtering Action Request related facets
    const facets =
        data?.listActionRequests?.facets?.filter((facet) =>
            (filterFacets || ACTION_REQUEST_DEFAULT_FACETS).includes(facet?.field || ''),
        ) || [];

    const actionRequests = useMemo(() => data?.listActionRequests?.actionRequests || [], [data]);
    const totalActionRequests = data?.listActionRequests?.total || 0;
    const hasPagination = totalActionRequests > pageSize;

    const onActionRequestUpdate = () => {
        refetch();
    };

    const FinalContainer = isShowNavBarRedesign ? Container : React.Fragment;

    return (
        <FinalContainer>
            {error && message.error('Failed to load proposals. An unknown error occurred!')}
            <ActionRequestsContainer $isShowNavBarRedesign={isShowNavBarRedesign} $height={height}>
                {title && <ActionRequestsTitle level={2}>{title}</ActionRequestsTitle>}
                {showFilters && (
                    <ProposalsTableHeader>
                        {/* TODO: Add entity search bar */}
                        <FilterSection
                            name="proposals"
                            loading={loading}
                            availableFilters={facets || []}
                            activeFilters={filters}
                            onChangeFilters={onChangeFilters}
                            customFilterLabels={PROPOSALS_FILTER_LABELS}
                            aggregationsEntityTypes={[EntityType.ActionRequest]}
                        />
                    </ProposalsTableHeader>
                )}
                <ProposalsTable
                    onRowClick={onProposalClick}
                    actionRequests={actionRequests as ActionRequest[]}
                    isLoading={loading}
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
