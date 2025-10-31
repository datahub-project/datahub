import React, { useState } from 'react';

import { SubscriptionListFilter } from '@app/settingsV2/personal/subscriptions/types';
import { Button } from '@src/alchemy-components';

import { useSearchSubscriptionsLazyQuery } from '@graphql/subscriptions.generated';
import { AndFilterInput, DataHubSubscription, EntityType, SortOrder } from '@types';

interface Props {
    selectedFilters: SubscriptionListFilter;
    orFilters: AndFilterInput[];
    totalSubscriptionsCount: number;
    setSelectedUrns: React.Dispatch<React.SetStateAction<string[]>>;
}

const BATCHED_SEARCH_COUNT = 500;

export const SubscriptionSelectAllBulkActionsBarItem = ({
    selectedFilters,
    orFilters,
    setSelectedUrns,
    totalSubscriptionsCount,
}: Props) => {
    const [isLoading, setIsLoading] = useState(false);
    const [searchSubscriptions] = useSearchSubscriptionsLazyQuery();

    const handleSelectAll = async () => {
        setIsLoading(true);

        try {
            const { searchText } = selectedFilters.filterCriteria;
            const query = searchText || '*';

            // Get first batch to determine total
            const firstResult = await searchSubscriptions({
                variables: {
                    input: {
                        types: [EntityType.Subscription],
                        query,
                        start: 0,
                        count: BATCHED_SEARCH_COUNT,
                        orFilters: orFilters.length > 0 ? orFilters : undefined,
                        sortInput: {
                            sortCriterion: {
                                field: '_id',
                                sortOrder: SortOrder.Ascending,
                            },
                        },
                    },
                },
            });

            const total = firstResult.data?.searchAcrossEntities?.total || 0;

            if (total === 0) {
                setIsLoading(false);
                return;
            }

            // Collect all URNs from first batch
            const allUrns: string[] =
                firstResult.data?.searchAcrossEntities?.searchResults
                    ?.map((result: any) => result.entity as DataHubSubscription)
                    .map((sub: DataHubSubscription) => sub.subscriptionUrn) || [];

            // Calculate remaining batches needed
            const remainingCount = total - BATCHED_SEARCH_COUNT;
            const additionalBatches = Math.ceil(remainingCount / BATCHED_SEARCH_COUNT);

            // Process additional batches sequentially if needed
            if (additionalBatches > 0) {
                const batchIndices = Array.from({ length: additionalBatches }, (_, i) => i + 1);

                await batchIndices.reduce(async (previousPromise, i) => {
                    await previousPromise;

                    const batchResult = await searchSubscriptions({
                        variables: {
                            input: {
                                types: [EntityType.Subscription],
                                query,
                                start: i * BATCHED_SEARCH_COUNT,
                                count: BATCHED_SEARCH_COUNT,
                                orFilters: orFilters.length > 0 ? orFilters : undefined,
                                sortInput: {
                                    sortCriterion: {
                                        field: '_id',
                                        sortOrder: SortOrder.Ascending,
                                    },
                                },
                            },
                        },
                    });

                    const batchUrns =
                        batchResult.data?.searchAcrossEntities?.searchResults
                            ?.map((result: any) => result.entity as DataHubSubscription)
                            .map((sub: DataHubSubscription) => sub.subscriptionUrn) || [];

                    allUrns.push(...batchUrns);
                }, Promise.resolve());
            }

            // Set all URNs as selected
            setSelectedUrns(allUrns);
        } catch (error) {
            console.error('Error selecting all subscriptions:', error);
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <Button variant="secondary" onClick={handleSelectAll} isLoading={isLoading} disabled={isLoading}>
            Select All ({totalSubscriptionsCount})
        </Button>
    );
};
