import { useCallback } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { ProductUpdate, latestUpdate } from '@app/shared/product/update/latestUpdate';
import { useAppConfig } from '@app/useAppConfig';

import { useBatchGetStepStatesQuery, useBatchUpdateStepStatesMutation } from '@graphql/step.generated';

const PRODUCT_UPDATE_STEP_PREFIX = 'product-updates';

/**
 * Determine whether product announcements feature is enabled and viewabled.
 */
export function useIsProductAnnouncementEnabled() {
    const appConfig = useAppConfig();
    const { showProductUpdates } = appConfig.config.featureFlags;
    return showProductUpdates;
}

/**
 * Hook to fetch the announcement data (eventually can replace with fetch).
 */
export function useGetLatestProductAnnouncementData() {
    return latestUpdate;
}

export type ProductAnnouncementResult = {
    visible: boolean;
    refetch: () => void;
};

/**
 * Hook to check if the announcement should be shown based on dismissal state
 */
export function useIsProductAnnouncementVisible(update: ProductUpdate): ProductAnnouncementResult {
    const userUrn = useUserContext()?.user?.urn;
    const productUpdateStepId = `${PRODUCT_UPDATE_STEP_PREFIX}-${update.id}`;
    const { data, loading, error, refetch } = useBatchGetStepStatesQuery({
        skip: !userUrn,
        variables: { input: { ids: [productUpdateStepId] } },
        fetchPolicy: 'cache-first',
    });

    console.log(`data is `, data);

    if (loading || error) {
        return {
            visible: false,
            refetch,
        };
    }

    const visible =
        (data?.batchGetStepStates?.results &&
            !data?.batchGetStepStates?.results?.some((result) => result?.id === productUpdateStepId)) ||
        false;

    return {
        visible,
        refetch,
    };
}

/**
 * Optional helper to dismiss the announcement (can also inline in `onClose`)
 */
export function useDismissProductAnnouncement(update: ProductUpdate, refetch: () => void): () => void {
    const userUrn = useUserContext()?.user?.urn;
    const productUpdateStepId = `${PRODUCT_UPDATE_STEP_PREFIX}-${update.id}`;
    const [batchUpdateStepStates] = useBatchUpdateStepStatesMutation();

    return useCallback(() => {
        if (!userUrn) return;

        const stepStates = [
            {
                id: productUpdateStepId,
                properties: [],
            },
        ];

        batchUpdateStepStates({
            variables: { input: { states: stepStates } },
        })
            .catch((error) => {
                console.error('Failed to dismiss product announcement:', error);
            })
            .finally(() => refetch());
    }, [userUrn, productUpdateStepId, batchUpdateStepStates, refetch]);
}
