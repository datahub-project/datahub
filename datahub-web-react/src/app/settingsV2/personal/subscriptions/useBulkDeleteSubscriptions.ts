import { useCallback, useRef, useState } from 'react';

import analytics, { EventType } from '@app/analytics';

import { useDeleteSubscriptionMutation } from '@graphql/subscriptions.generated';
import { DataHubSubscription } from '@types';

export type BulkDeleteProgress = {
    hasStarted: boolean;
    total: number;
    completed: number;
    successful: string[];
    failed: Array<{ urn: string; error: string }>;
};

const DEFAULT_PROGRESS: BulkDeleteProgress = {
    hasStarted: false,
    total: 0,
    completed: 0,
    successful: [],
    failed: [],
};

const CHUNK_SIZE = 10; // Process deletions in small chunks to avoid overwhelming the server

/**
 * Hook to bulk delete subscriptions with progress tracking.
 * @returns The progress tracker and the bulk delete function.
 */
export const useBulkDeleteSubscriptions = () => {
    const [deleteSubscriptionMutation] = useDeleteSubscriptionMutation();
    const [progress, setProgress] = useState<BulkDeleteProgress>(DEFAULT_PROGRESS);
    const progressRef = useRef<BulkDeleteProgress>(progress);
    progressRef.current = progress;

    const deleteSubscription = useCallback(
        async (subscriptionUrn: string): Promise<{ success: boolean; error?: string }> => {
            try {
                await deleteSubscriptionMutation({
                    variables: {
                        input: { subscriptionUrn },
                    },
                    fetchPolicy: 'no-cache',
                });
                return { success: true };
            } catch (error) {
                const errorMessage = error instanceof Error ? error.message : 'Unknown error';
                return { success: false, error: errorMessage };
            }
        },
        [deleteSubscriptionMutation],
    );

    const resetProgress = useCallback(() => {
        setProgress(DEFAULT_PROGRESS);
    }, []);

    const bulkDeleteSubscriptionsByUrns = useCallback(
        async (subscriptionUrns: string[], isPersonal: boolean) => {
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
                    type: EventType.BulkSubscriptionDeleteEvent,
                    subscriptionCount: subscriptionUrns.length,
                    isPersonal,
                });
            } catch (error) {
                console.error('Error sending bulk delete event', error);
            }

            // Process deletions in chunks to avoid rate limiting
            const chunks: string[][] = [];
            for (let i = 0; i < subscriptionUrns.length; i += CHUNK_SIZE) {
                chunks.push(subscriptionUrns.slice(i, i + CHUNK_SIZE));
            }

            // Process chunks sequentially, but items within each chunk in parallel
            await chunks.reduce(async (previousChunkPromise, chunk) => {
                await previousChunkPromise;

                const results = await Promise.allSettled(
                    chunk.map(async (subscriptionUrn) => {
                        const result = await deleteSubscription(subscriptionUrn);
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
                    type: EventType.BulkSubscriptionDeleteCompletedEvent,
                    subscriptionCount: subscriptionUrns.length,
                    successfulCount: progressRef.current.successful.length,
                    failedCount: progressRef.current.failed.length,
                    isPersonal,
                });
            } catch (error) {
                console.error('Error sending bulk delete completed event', error);
            }

            return progressRef.current;
        },
        [deleteSubscription],
    );

    const bulkDeleteSubscriptions = useCallback(
        async (subscriptions: DataHubSubscription[], isPersonal: boolean) => {
            const subscriptionUrns = subscriptions.map((s) => s.subscriptionUrn);
            return bulkDeleteSubscriptionsByUrns(subscriptionUrns, isPersonal);
        },
        [bulkDeleteSubscriptionsByUrns],
    );

    return {
        progress,
        bulkDeleteSubscriptions,
        bulkDeleteSubscriptionsByUrns,
        resetProgress,
    };
};
