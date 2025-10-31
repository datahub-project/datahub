import { PageTitle, Pagination } from '@components';
import { Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import { Checkbox } from '@components/components/Checkbox';
import { Column, Table } from '@components/components/Table';

import { useUserContext } from '@app/context/useUserContext';
import { TableLoadingSkeleton } from '@app/entityV2/shared/TableLoadingSkeleton';
import { ENABLE_UPSTREAM_NOTIFICATIONS } from '@app/settingsV2/personal/notifications/constants';
import { SubscriptionBulkActionsBar } from '@app/settingsV2/personal/subscriptions/SubscriptionBulkActionsBar';
import { SubscriptionListFilters } from '@app/settingsV2/personal/subscriptions/SubscriptionListFilters';
import { SUBSCRIPTION_DEFAULT_FILTERS } from '@app/settingsV2/personal/subscriptions/constants';
import { SubscriptionActions } from '@app/settingsV2/personal/subscriptions/table/SubscriptionActions';
import SubscriptionsChannelColumn from '@app/settingsV2/personal/subscriptions/table/columns/SubscriptionsChannelColumn';
import SubscriptionsEntityChangeTypesColumn from '@app/settingsV2/personal/subscriptions/table/columns/SubscriptionsEntityChangeTypesColumn';
import SubscriptionsEntityColumn from '@app/settingsV2/personal/subscriptions/table/columns/SubscriptionsEntityColumn';
import SubscriptionsOwnerColumn from '@app/settingsV2/personal/subscriptions/table/columns/SubscriptionsOwnerColumn';
import SubscriptionsSubscribedSinceColumn from '@app/settingsV2/personal/subscriptions/table/columns/SubscriptionsSubscribedSinceColumn';
import SubscriptionsUpstreamsColumn from '@app/settingsV2/personal/subscriptions/table/columns/SubscriptionsUpstreamsColumn';
import { SubscriptionListFilter } from '@app/settingsV2/personal/subscriptions/types';
import { scrollToTop } from '@app/shared/searchUtils';
import useActorSinkSettings from '@app/shared/subscribe/drawer/useSinkSettings';

import { useGetOwnedGroupsQuery } from '@graphql/group.generated';
import { useSearchSubscriptionsQuery } from '@graphql/subscriptions.generated';
import { useGetUserGroupsQuery } from '@graphql/user.generated';
import {
    AndFilterInput,
    CorpGroup,
    CorpUser,
    DataHubSubscription,
    EntityType,
    FacetFilterInput,
    FilterOperator,
    SortOrder,
} from '@types';

import EmptySimpleSvg from '@images/empty-simple.svg?react';

const PAGE_SIZE = 10;

const ColumnTitle = styled.div`
    font-family: 'Mulish', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 700;
    display: flex;
    align-items: center;
`;

const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 12px;
    padding-top: 40px;
    padding-bottom: 40px;
`;

const EmptySubscriptionsText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
    color: #595959;
`;

const SubscriptionListContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin: 16px 20px;
    flex: 1;
    overflow: hidden;
    min-height: 0;
    gap: 20px 0px;
`;

const SubscriptionContentWrapper = styled.div`
    position: relative;
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    min-height: 0;
`;

const StyledPagination = styled(Pagination)`
    margin-bottom: 0;
`;
const NUM_GROUP_URNS_TO_FETCH = 100;

const buildOrFilters = (
    selectedFilters: SubscriptionListFilter,
    urn: string,
    groupUrns: string[],
    excludedSubscriptionUrns: string[] = [],
): AndFilterInput[] => {
    const filters: FacetFilterInput[] = [];
    // Add other filters
    const { entity, owner, eventType } = selectedFilters.filterCriteria;

    if (owner.length > 0) {
        filters.push({
            field: 'actorUrn',
            values: owner,
            condition: FilterOperator.Equal,
        });
    } else if (groupUrns.length > 0) {
        filters.push({
            field: 'actorUrn',
            values: [urn, ...groupUrns],
            condition: FilterOperator.Equal,
        });
    } else {
        filters.push({
            field: 'actorUrn',
            values: [urn],
            condition: FilterOperator.Equal,
        });
    }

    if (entity.length > 0) {
        filters.push({
            field: 'entityUrn',
            values: entity,
            condition: FilterOperator.Contain,
        });
    }

    if (eventType.length > 0) {
        filters.push({
            field: 'entityChangeTypes',
            values: eventType,
            condition: FilterOperator.Equal,
        });
    }

    if (excludedSubscriptionUrns.length > 0) {
        filters.push({
            field: 'subscriptionUrn',
            values: excludedSubscriptionUrns,
            condition: FilterOperator.Equal,
            negated: true,
        });
    }

    return [
        {
            and: filters,
        },
    ];
};

type Props =
    | {
          isPersonal: true;
          groupUrn?: undefined;
      }
    | {
          isPersonal: false;
          groupUrn: string;
      };

type ManageActorSubscriptionsContentProps =
    | {
          user: CorpUser;
          isPersonal: true;
          groupUrn?: string;
      }
    | {
          user: CorpUser;
          isPersonal: false;
          groupUrn: string;
      };

/**
 * Main content component that handles subscription management logic.
 * Only rendered after user is loaded.
 */
const ManageActorSubscriptionsContent: React.FC<ManageActorSubscriptionsContentProps> = ({
    user,
    isPersonal,
    groupUrn,
}) => {
    const userUrn = user.urn;
    const actorUrn = isPersonal ? userUrn : groupUrn;
    const { data: userGroupsData, loading: isUserGroupsLoading } = useGetUserGroupsQuery({
        variables: { urn: userUrn, start: 0, count: NUM_GROUP_URNS_TO_FETCH },
        skip: !isPersonal,
        fetchPolicy: 'cache-and-network',
    });
    const { data: ownedGroupsData, loading: isOwnedGroupsLoading } = useGetOwnedGroupsQuery({
        variables: { userUrn, start: 0, count: NUM_GROUP_URNS_TO_FETCH },
        skip: !isPersonal,
        fetchPolicy: 'cache-and-network',
    });

    const [page, setPage] = useState(1);
    const [selectedFilters, setSelectedFilters] = useState<SubscriptionListFilter>(SUBSCRIPTION_DEFAULT_FILTERS);
    const [selectedSubscriptionUrns, setSelectedSubscriptionUrns] = useState<string[]>([]);

    // Reset page to 1 when filters change, since we paginate results
    useEffect(() => {
        setPage(1);
    }, [
        selectedFilters.filterCriteria.searchText,
        selectedFilters.filterCriteria.entity,
        selectedFilters.filterCriteria.owner,
        selectedFilters.filterCriteria.eventType,
    ]);

    const memberGroups: CorpGroup[] =
        userGroupsData?.corpUser?.relationships?.relationships?.map((r) => r.entity as CorpGroup) || [];
    const ownedGroups: CorpGroup[] =
        ownedGroupsData?.search?.searchResults?.map((result) => result.entity as CorpGroup) || [];
    const memberGroupUrns: string[] = memberGroups.map((r) => r.urn);
    const ownedGroupUrns: string[] = ownedGroups.map((r) => r.urn);

    // Combine & dedupe both member and owned group urns. This is because a user
    // can be an owner but not a member of the group.
    const groupUrns: string[] = [...new Set([...memberGroupUrns, ...ownedGroupUrns])];

    // Get sink settings for the current group (for group subscriptions) or user (for personal subscriptions)
    const { notificationSettings: actorNotificationSettings, loading: isSinkSettingsLoading } = useActorSinkSettings({
        isPersonal,
        groupUrn,
    });
    const orFilters = buildOrFilters(selectedFilters, actorUrn, groupUrns);
    const { searchText } = selectedFilters.filterCriteria;
    const start = (page - 1) * PAGE_SIZE;
    const searchVariables = {
        types: [EntityType.Subscription],
        query: searchText || '*',
        start,
        count: PAGE_SIZE,
        orFilters: orFilters.length > 0 ? orFilters : undefined,
        sortInput: { sortCriterion: { field: 'createdOn', sortOrder: SortOrder.Descending } },
    };
    const {
        data: searchResults,
        refetch,
        loading: isSubscriptionsLoading,
    } = useSearchSubscriptionsQuery({
        variables: { input: searchVariables },
        skip: isPersonal && (isUserGroupsLoading || isOwnedGroupsLoading),
        fetchPolicy: 'no-cache',
    });

    const subscriptions = (searchResults?.searchAcrossEntities?.searchResults?.map((result) => result.entity) ||
        []) as DataHubSubscription[];
    const actorUrnFacet = searchResults?.searchAcrossEntities?.facets?.find((facet) => facet.field === 'actorUrn');
    const allSubscriptionOwners: (CorpUser | CorpGroup)[] =
        actorUrnFacet?.aggregations?.map((aggregation) => aggregation.entity as CorpUser | CorpGroup) || [];
    const numSubscriptions = searchResults?.searchAcrossEntities?.total || 0;

    // Adjust page if current page is beyond available pages after deletion
    // This handles the case where deletions cause the current page to become empty
    useEffect(() => {
        if (isSubscriptionsLoading) return; // Don't adjust while loading

        if (numSubscriptions === 0) {
            // No subscriptions left, go to page 1
            if (page > 1) {
                setPage(1);
            }
        }

        if (subscriptions.length > 0) {
            return;
        }

        if (page > 1) {
            // Current page is empty but there are still subscriptions elsewhere
            // This happens when all items on the current page are deleted
            const maxPage = Math.ceil(numSubscriptions / PAGE_SIZE);
            if (page > maxPage) {
                // Navigate to the last valid page
                setPage(maxPage);
            } else if (maxPage >= 1) {
                // If we're on a page that should have items but doesn't,
                // go to the previous page (or page 1 if we're already on page 2)
                const newPage = Math.max(1, page - 1);
                setPage(newPage);
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isSubscriptionsLoading, numSubscriptions, subscriptions.length, page, refetch]);

    /**
     * This function needs to be invoked after bulk deleting subscriptions, as
     * the filtration of deleted subscriptions happens in the graphql layer,
     * resulting in the current page having fewer than PAGE_SIZE subscriptions.
     */
    const refetchExcludingUrns = async (urns?: string[]) => {
        const newOrFilters = buildOrFilters(selectedFilters, actorUrn, groupUrns, urns);
        await refetch({ input: { ...searchVariables, orFilters: newOrFilters } });
    };

    const handleFilterChange = (filter: SubscriptionListFilter) => {
        setSelectedFilters(filter);
    };

    // Get all subscription URNs on current page
    const currentPageSubscriptionUrns = subscriptions.map((s) => s.subscriptionUrn);
    const allCurrentPageSelected =
        currentPageSubscriptionUrns.length > 0 &&
        currentPageSubscriptionUrns.every((urn) => selectedSubscriptionUrns.includes(urn));
    const someCurrentPageSelected = currentPageSubscriptionUrns.some((urn) => selectedSubscriptionUrns.includes(urn));

    const handleSelectAll = (checked: boolean) => {
        if (checked) {
            // Add all current page subscriptions to selection
            const newUrns = [...new Set([...selectedSubscriptionUrns, ...currentPageSubscriptionUrns])];
            setSelectedSubscriptionUrns(newUrns);
        } else {
            // Remove all current page subscriptions from selection
            setSelectedSubscriptionUrns((prev) => prev.filter((urn) => !currentPageSubscriptionUrns.includes(urn)));
        }
    };

    const subscriptionTableColumns: Column<DataHubSubscription>[] = [
        {
            title: (
                <Checkbox
                    size="xs"
                    isChecked={allCurrentPageSelected}
                    isIntermediate={someCurrentPageSelected && !allCurrentPageSelected}
                    onCheckboxChange={handleSelectAll}
                />
            ),
            key: 'select',
            dataIndex: 'select',
            render: (subscription: DataHubSubscription) => (
                <Checkbox
                    size="xs"
                    isChecked={selectedSubscriptionUrns.includes(subscription.subscriptionUrn)}
                    onCheckboxChange={(checked) => {
                        if (checked) {
                            setSelectedSubscriptionUrns((prev) => [...prev, subscription.subscriptionUrn]);
                        } else {
                            setSelectedSubscriptionUrns((prev) =>
                                prev.filter((urn) => urn !== subscription.subscriptionUrn),
                            );
                        }
                    }}
                />
            ),
            width: '60px',
        },
        {
            title: <ColumnTitle>Name</ColumnTitle>,
            key: 'name',
            dataIndex: 'name',
            render: (subscription: DataHubSubscription) => <SubscriptionsEntityColumn subscription={subscription} />,
        },
        {
            title: <ColumnTitle>Destinations</ColumnTitle>,
            key: 'channels',
            dataIndex: 'channels',
            render: (subscription: DataHubSubscription) => (
                <SubscriptionsChannelColumn
                    subscription={subscription}
                    actorUrn={actorUrn}
                    ownedAndMemberGroup={ownedGroups.concat(memberGroups)}
                    actorNotificationSettings={actorNotificationSettings || undefined}
                />
            ),
        },
        {
            title: <ColumnTitle>Owner</ColumnTitle>,
            key: 'actorUrn',
            dataIndex: 'actorUrn',
            render: (subscription: DataHubSubscription) => <SubscriptionsOwnerColumn subscription={subscription} />,
        },
        {
            title: <ColumnTitle>Events</ColumnTitle>,
            key: 'entityChangeTypes',
            dataIndex: 'entityChangeTypes',
            render: (subscription: DataHubSubscription) => (
                <SubscriptionsEntityChangeTypesColumn subscription={subscription} />
            ),
        },
        ...(ENABLE_UPSTREAM_NOTIFICATIONS
            ? [
                  {
                      title: <ColumnTitle>Subscribed to Upstreams</ColumnTitle>,
                      key: 'upstreams',
                      dataIndex: 'upstreams',
                      render: (subscription: DataHubSubscription) => (
                          <SubscriptionsUpstreamsColumn subscription={subscription} />
                      ),
                  },
              ]
            : []),
        {
            title: <ColumnTitle>Created</ColumnTitle>,
            key: 'since',
            dataIndex: 'since',
            render: (subscription: DataHubSubscription) => (
                <SubscriptionsSubscribedSinceColumn subscription={subscription} />
            ),
        },
        {
            title: '',
            key: 'actions',
            dataIndex: 'actions',
            render: (subscription: DataHubSubscription) => (
                <SubscriptionActions subscription={subscription} refetchListSubscriptions={refetch} />
            ),
        },
    ];

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    if ((isPersonal && (isUserGroupsLoading || isOwnedGroupsLoading)) || isSinkSettingsLoading) {
        return <TableLoadingSkeleton />;
    }

    const hasResults = subscriptions.length > 0;
    // To avoid the list jumping around, we will display the stale data while
    // refetching the new data
    const hasSearchQuery = searchText.trim() !== '';
    const { entity, owner, eventType } = selectedFilters.filterCriteria;
    const hasActiveFilters = entity.length > 0 || owner.length > 0 || eventType.length > 0;
    const hasUserAppliedRefinements = hasSearchQuery || hasActiveFilters;
    const refinementReturnedNoResults = !isSubscriptionsLoading && !hasResults && hasUserAppliedRefinements;

    return (
        <SubscriptionContentWrapper>
            <SubscriptionListFilters
                subscriptions={subscriptions}
                allSubscriptionOwners={allSubscriptionOwners}
                selectedFilters={selectedFilters}
                handleFilterChange={handleFilterChange}
                viewer={user}
                ownedAndMemberGroupUrns={groupUrns}
            />
            <SubscriptionBulkActionsBar
                selectedUrns={selectedSubscriptionUrns}
                setSelectedUrns={setSelectedSubscriptionUrns}
                refetch={refetchExcludingUrns}
                isPersonal={isPersonal}
                hasPagination={numSubscriptions >= PAGE_SIZE}
                selectedFilters={selectedFilters}
                orFilters={orFilters}
                totalSubscriptionsCount={numSubscriptions}
            />
            {!isSubscriptionsLoading && !hasResults && !hasUserAppliedRefinements ? (
                <EmptyContainer>
                    <EmptySimpleSvg />
                    <EmptySubscriptionsText>
                        You are not currently subscribed to any entities. Get started by subscribing to entities most
                        relevant to you.
                    </EmptySubscriptionsText>
                </EmptyContainer>
            ) : null}
            {refinementReturnedNoResults ? (
                <EmptyContainer>
                    <EmptySimpleSvg />
                    <EmptySubscriptionsText>
                        No subscriptions match your current filters and search criteria.
                    </EmptySubscriptionsText>
                </EmptyContainer>
            ) : null}
            {(subscriptions.length > 0 || isSubscriptionsLoading) && (
                <>
                    <Table
                        columns={subscriptionTableColumns}
                        isScrollable
                        data={subscriptions}
                        isLoading={isSubscriptionsLoading}
                        rowKey={(record) => record.subscriptionUrn}
                    />
                    {(numSubscriptions >= PAGE_SIZE || !isSubscriptionsLoading) && (
                        <StyledPagination
                            currentPage={page}
                            itemsPerPage={PAGE_SIZE}
                            total={numSubscriptions}
                            showLessItems
                            onPageChange={onChangePage}
                            showSizeChanger={false}
                        />
                    )}
                </>
            )}
        </SubscriptionContentWrapper>
    );
};

export const ManageActorSubscriptions = ({ isPersonal, groupUrn }: Props) => {
    const { user, loaded: isUserLoaded } = useUserContext();
    const pageTitle = isPersonal ? 'My Subscriptions' : 'Group Subscriptions';

    if (!isUserLoaded || !user) {
        return (
            <SubscriptionListContainer>
                <PageTitle title={pageTitle} subTitle="Manage your personal and group subscriptions" />
                <TableLoadingSkeleton />
            </SubscriptionListContainer>
        );
    }

    return (
        <SubscriptionListContainer>
            <PageTitle title={pageTitle} subTitle="Manage your personal and group subscriptions" />
            {isPersonal ? (
                <ManageActorSubscriptionsContent user={user} isPersonal />
            ) : (
                <ManageActorSubscriptionsContent user={user} isPersonal={false} groupUrn={groupUrn} />
            )}
        </SubscriptionListContainer>
    );
};
