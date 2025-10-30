import { useCallback, useRef, useState } from 'react';

import analytics, { EventType } from '@app/analytics';

import { useUpdateSubscriptionMutation } from '@graphql/subscriptions.generated';
import { EntityChangeDetailsInput } from '@types';

export type BulkUpdateProgress = {
    hasStarted: boolean;
    total: number;
    completed: number;
    successful: string[];
    failed: Array<{ urn: string; error: string }>;
};

const DEFAULT_PROGRESS: BulkUpdateProgress = {
    hasStarted: false,
    total: 0,
    completed: 0,
    successful: [],
    failed: [],
};

const CHUNK_SIZE = 10; // Process updates in small chunks to avoid overwhelming the server

/**
 * Hook to bulk update subscriptions with progress tracking.
 * @returns The progress tracker and the bulk update function.
 */
export const useBulkUpdateSubscriptions = () => {
    const [updateSubscriptionMutation] = useUpdateSubscriptionMutation();
    const [progress, setProgress] = useState<BulkUpdateProgress>(DEFAULT_PROGRESS);
    const progressRef = useRef<BulkUpdateProgress>(progress);
    progressRef.current = progress;

    const updateSubscription = useCallback(
        async (
            subscriptionUrn: string,
            entityChangeTypes: EntityChangeDetailsInput[],
        ): Promise<{ success: boolean; error?: string }> => {
            try {
                await updateSubscriptionMutation({
                    variables: {
                        input: {
                            subscriptionUrn,
                            entityChangeTypes,
                        },
                    },
                    fetchPolicy: 'no-cache',
                });
                return { success: true };
            } catch (error) {
                const errorMessage = error instanceof Error ? error.message : 'Unknown error';
                return { success: false, error: errorMessage };
            }
        },
        [updateSubscriptionMutation],
    );

    const resetProgress = useCallback(() => {
        setProgress(DEFAULT_PROGRESS);
    }, []);

    const bulkUpdateSubscriptionsByUrns = useCallback(
        async (subscriptionUrns: string[], entityChangeTypes: EntityChangeDetailsInput[], isPersonal: boolean) => {
            // Initialize progress
            setProgress({
                hasStarted: true,
                total: subscriptionUrns.length,
                completed: 0,
                successful: [],
                failed: [],
            });

            // Track analytics event
            try {
                analytics.event({
                    type: EventType.BulkSubscriptionUpdateEvent,
                    subscriptionCount: subscriptionUrns.length,
                    isPersonal,
                });
            } catch (error) {
                console.error('Error sending bulk update event', error);
            }

            // Process updates in chunks to avoid rate limiting
            const chunks: string[][] = [];
            for (let i = 0; i < subscriptionUrns.length; i += CHUNK_SIZE) {
                chunks.push(subscriptionUrns.slice(i, i + CHUNK_SIZE));
            }

            // Process chunks sequentially, but items within each chunk in parallel
            await chunks.reduce(async (previousChunkPromise, chunk) => {
                await previousChunkPromise;

                const results = await Promise.allSettled(
                    chunk.map(async (subscriptionUrn) => {
                        const result = await updateSubscription(subscriptionUrn, entityChangeTypes);
                        return { subscriptionUrn, result };
                    }),
                );

                // Update progress after each chunk
                results.forEach((settledResult) => {
                    if (settledResult.status === 'fulfilled') {
                        const { subscriptionUrn, result } = settledResult.value;
                        if (result.success) {
                            setProgress((prev) => ({
                                ...prev,
                                completed: prev.completed + 1,
                                successful: [...prev.successful, subscriptionUrn],
                            }));
                        } else {
                            setProgress((prev) => ({
                                ...prev,
                                completed: prev.completed + 1,
                                failed: [
                                    ...prev.failed,
                                    { urn: subscriptionUrn, error: result.error || 'Unknown error' },
                                ],
                            }));
                        }
                    }
                });
            }, Promise.resolve());

            // Send completion analytics
            try {
                analytics.event({
                    type: EventType.BulkSubscriptionUpdateCompletedEvent,
                    subscriptionCount: subscriptionUrns.length,
                    successfulCount: progressRef.current.successful.length,
                    failedCount: progressRef.current.failed.length,
                    isPersonal,
                });
            } catch (error) {
                console.error('Error sending bulk update completed event', error);
            }

            return progressRef.current;
        },
        [updateSubscription],
    );

    return {
        progress,
        bulkUpdateSubscriptionsByUrns,
        resetProgress,
    };
};
