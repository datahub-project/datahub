import React, { useMemo, useState } from 'react';
import { message, Typography } from 'antd';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { SearchBar, Pagination } from '@src/alchemy-components';
import FilterSection from '@src/app/sharedV2/filters/FilterSection';
import usePagination from '@src/app/sharedV2/pagination/usePagination';
import ProposalsTable from './proposalsTable/ProposalsTable';
import { ActionRequest, ActionRequestAssignee, EntityType } from '../../../types.generated';
import { useListActionRequestsQuery } from '../../../graphql/actionRequest.generated';
import ActionsBar from './ActionsBar';
import { ACTION_REQUEST_DEFAULT_FACETS, PROPOSALS_FILTER_LABELS } from '../utils/constants';
import { MY_PROPOSALS_GROUP_NAME } from './utils';
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
    groupName?: string;
    userUrn?: string;
    resourceUrn?: string;
    showFilters?: boolean;
    useUrlParams?: boolean;
    height?: string;
};

export const ProposalList = ({
    title,
    assignee,
    onProposalClick,
    groupName,
    userUrn,
    resourceUrn,
    showFilters = false,
    useUrlParams = true,
    height,
}: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [selectedUrns, setSelectedUrns] = useState<string[]>([]);
    const { start, pageSize, setPageSize, page, setPage } = usePagination(DEFAULT_PAGE_SIZE);
    const { filters, orFilters, onChangeFilters } = useGetActionRequestsQueryInputs({ useUrlParams });

    const { loading, error, data, refetch } = useListActionRequestsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                orFilters,
                assignee,
                resourceUrn,
                // facets can be converted to prop if needed.
                facets: ACTION_REQUEST_DEFAULT_FACETS,
            },
        },
        fetchPolicy: 'no-cache',
    });

    // TODO: Remove this filtering if we are passing facets to the API
    const facets =
        data?.listActionRequests?.facets?.filter((facet) =>
            ACTION_REQUEST_DEFAULT_FACETS.includes(facet?.field || ''),
        ) || [];

    let actionRequests = useMemo(() => data?.listActionRequests?.actionRequests || [], [data]);

    // TODO: Should we do fetch by userUrn instead? We have to reset the filters anyway
    if (groupName === MY_PROPOSALS_GROUP_NAME) {
        actionRequests = actionRequests.filter((request) => request.created.actor?.urn === userUrn);
    }
    const totalActionRequests = data?.listActionRequests?.total || 0;

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
                        {/* TODO: Repleace SearchBar with Entity Search option here */}
                        <SearchBar />
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
                />
                <FooterContainer>
                    {selectedUrns.length > 0 && (
                        <ActionsBar
                            selectedUrns={selectedUrns}
                            setSelectedUrns={setSelectedUrns}
                            onActionRequestUpdate={onActionRequestUpdate}
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
