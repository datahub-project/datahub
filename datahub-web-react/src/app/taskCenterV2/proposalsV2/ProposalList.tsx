import React, { useMemo, useState } from 'react';
import { message, Typography } from 'antd';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { navigateWithFilters } from '@src/app/sharedV2/filters/navigateWithFilters';
import { useHistory, useLocation } from 'react-router';
import { SearchBar, Pagination } from '@src/alchemy-components';
import FilterSection from '@src/app/sharedV2/filters/FilterSection';
import { useListActionRequestsQuery } from '../../../graphql/actionRequest.generated';
import useGetActionRequestsQueryInputs from './useGetActionRequestsQueryInputs';
import ProposalsTable from './proposalsTable/ProposalsTable';
import { ActionRequest, ActionRequestAssignee, EntityType, FacetFilterInput } from '../../../types.generated';
import ActionsBar from './ActionsBar';
import { ACTION_REQUEST_DEFAULT_FACETS, PROPOSALS_FILTER_LABELS } from '../utils/constants';
import { MY_PROPOSALS_GROUP_NAME } from './utils';

const ActionRequestsContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    overflow: hidden;
    flex: 1;
    display: flex;
    flex-direction: column;
    ${(props) => props.$isShowNavBarRedesign && 'height: calc(100% - 200px);'}
    margin: 20px;
`;

const Container = styled.div`
    display: contents;
`;

const FooterContainer = styled.div`
    margin-top: 15px;
`;

const ActionRequestsTitle = styled(Typography.Title)`
    && {
        margin-bottom: 24px;
    }
`;

const DEFAULT_PAGE_SIZE = 25;

const ProposalsTableHeader = styled.div`
    display: flex;
    justify-content: space-between;
`;

type Props = {
    title?: string;
    assignee?: ActionRequestAssignee;
    groupName?: string;
    userUrn?: string;
};

export const ProposalList = ({ title, assignee, groupName, userUrn }: Props) => {
    const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { page, orFilters, filters } = useGetActionRequestsQueryInputs();
    const history = useHistory();
    const location = useLocation();
    const [selectedUrns, setSelectedUrns] = useState<string[]>([]);

    // Policy list paging.
    const start = (page - 1) * pageSize;

    const { loading, error, data, refetch } = useListActionRequestsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                orFilters,
                assignee,
                // This can be converted to prop if needed.
                facets: ACTION_REQUEST_DEFAULT_FACETS,
            },
        },
        fetchPolicy: 'no-cache',
    });

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        navigateWithFilters({
            filters: newFilters,
            page,
            history,
            location,
        });
    };

    const onChangePage = (newPage: number) => {
        navigateWithFilters({
            filters,
            page: newPage,
            history,
            location,
        });
    };

    // TODO: Remove this filtering if we are passing facets to the API
    const facets =
        data?.listActionRequests?.facets?.filter((facet) =>
            ACTION_REQUEST_DEFAULT_FACETS.includes(facet?.field || ''),
        ) || [];

    let actionRequests = useMemo(() => data?.listActionRequests?.actionRequests || [], [data]);

    // TODO: Should we do fetch by resource Urn instead? We have to reset the filters anyway
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
            <ActionRequestsContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
                {title && <ActionRequestsTitle level={2}>{title}</ActionRequestsTitle>}
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
                <ProposalsTable
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
                        onPageChange={onChangePage}
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
