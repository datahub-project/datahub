import { useCallback } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useAppConfig } from '@app/useAppConfig';

import { useGetLatestProductUpdateQuery } from '@graphql/app.generated';
import { useBatchGetStepStatesQuery, useBatchUpdateStepStatesMutation } from '@graphql/step.generated';

const PRODUCT_UPDATE_STEP_PREFIX = 'product_updates';

function buildProductUpdateStepId(userUrn: string, updateId: string): string {
    return `${userUrn}-${PRODUCT_UPDATE_STEP_PREFIX}-${updateId}`;
}

/**
 * Determine whether product announcements feature is enabled and viewabled.
 */
export function useIsProductAnnouncementEnabled() {
    const appConfig = useAppConfig();
    const { showProductUpdates } = appConfig.config.featureFlags;
    return showProductUpdates;
}

/**
 * Hook to fetch the latest product announcement data from GraphQL.
 */
export function useGetLatestProductAnnouncementData() {
    const { data, loading, error } = useGetLatestProductUpdateQuery({
        fetchPolicy: 'cache-first',
    });

    // Return null if loading, error, or no data
    if (loading || error || !data?.latestProductUpdate) {
        return null;
    }

    return data.latestProductUpdate;
}

export type ProductAnnouncementResult = {
    visible: boolean;
    refetch: () => void;
};

/**
 * Hook to check if the announcement should be shown based on dismissal state
 */
export function useIsProductAnnouncementVisible(updateId: string | null | undefined): ProductAnnouncementResult {
    const userUrn = useUserContext()?.user?.urn;
    const productUpdateStepId = userUrn && updateId ? buildProductUpdateStepId(userUrn, updateId) : null;
    const productUpdateStepIds = productUpdateStepId ? [productUpdateStepId] : [];
    const { data, loading, error, refetch } = useBatchGetStepStatesQuery({
        skip: !userUrn || !updateId,
        variables: { input: { ids: productUpdateStepIds } },
        fetchPolicy: 'cache-first',
    });

    // If userUrn is not loaded yet, don't show the announcement (wait for user context to load)
    if (!userUrn) {
        return {
            visible: false,
            refetch,
        };
    }

    // If updateId is not available, don't show
    if (!updateId) {
        return {
            visible: false,
            refetch,
        };
    }

    // If query is loading or has an error, don't show yet
    if (loading || error) {
        return {
            visible: false,
            refetch,
        };
    }

    // Show announcement if the step state doesn't exist (user hasn't dismissed it)
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
export function useDismissProductAnnouncement(updateId: string | null | undefined, refetch: () => void): () => void {
    const userUrn = useUserContext()?.user?.urn;
    const productUpdateStepId = userUrn && updateId ? buildProductUpdateStepId(userUrn, updateId) : null;

    const [batchUpdateStepStates] = useBatchUpdateStepStatesMutation();

    return useCallback(() => {
        if (!productUpdateStepId) return;

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
    }, [productUpdateStepId, batchUpdateStepStates, refetch]);
}
