import { useCallback } from 'react';
import { useTranslation } from 'react-i18next';

import { useUserContext } from '@app/context/useUserContext';
import { checkShouldSkipWelcomeModal, dismissProductUpdate } from '@app/shared/localStorageUtils';
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
    const { i18n } = useTranslation();
    const { data, loading, error } = useGetLatestProductUpdateQuery({
        variables: { locale: i18n.language },
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
    /** True until the server has answered whether this user dismissed the update. */
    pending: boolean;
    /** True only when the server step state says this update was dismissed. */
    dismissed: boolean;
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

    const unanswered = { visible: false, pending: true, dismissed: false, refetch };

    // If userUrn is not loaded yet, don't show the announcement (wait for user context to load)
    if (!userUrn) {
        return unanswered;
    }

    // If welcome modal hasn't been seen/dismissed, don't show product update
    const hasSeenWelcomeModal = checkShouldSkipWelcomeModal();
    if (!hasSeenWelcomeModal) {
        return {
            visible: false,
            pending: false,
            dismissed: false,
            refetch,
        };
    }

    // If updateId is not available, don't show
    if (!updateId) {
        return unanswered;
    }

    // If query is loading or has an error, don't show yet
    if (loading || error) {
        return {
            visible: false,
            pending: !error,
            dismissed: false,
            refetch,
        };
    }

    const dismissed =
        data?.batchGetStepStates?.results?.some((result) => result?.id === productUpdateStepId) || false;

    return {
        visible: !dismissed && !!data?.batchGetStepStates?.results,
        pending: false,
        dismissed,
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
        if (updateId) {
            dismissProductUpdate(updateId);
        }
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
    }, [updateId, productUpdateStepId, batchUpdateStepStates, refetch]);
}
