import { useRef, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import {
    getDefaultVolumeSourceType,
    getVolumeSourceTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/utils';
import {
    getDefaultFreshnessSourceOption,
    getFreshnessSourceOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/utils';
import { BulkCreateDatasetAssertionsSpec, ProgressTracker } from '@app/observe/shared/bulkCreate/constants';
import { SearchFunction, performBatchedSearchWithUrnSorting } from '@app/observe/shared/bulkCreate/searchUtils';

import {
    UpsertDatasetFreshnessAssertionMonitorMutationFn,
    UpsertDatasetVolumeAssertionMonitorMutationFn,
    useUpsertDatasetFreshnessAssertionMonitorMutation,
    useUpsertDatasetVolumeAssertionMonitorMutation,
} from '@graphql/assertion.generated';
import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { SyncSubscriptionMutationFn, useSyncSubscriptionMutation } from '@graphql/subscriptions.generated';
import {
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionType,
    Dataset,
    DatasetFreshnessAssertionParametersInput,
    DatasetFreshnessSourceType,
    DatasetVolumeAssertionParametersInput,
    DatasetVolumeSourceType,
    MonitorMode,
    VolumeAssertionType,
} from '@types';

const DEFAULT_PROGRESS_TRACKER: ProgressTracker = {
    hasFetched: false,
    total: 0,
    completed: 0,
    successful: [],
    errored: [],
};

const CHUNK_SIZE = 100;

/**
 * Divides an array of datasets into chunks of the specified size.
 * @param datasets - The array of datasets to chunk.
 * @returns An array of dataset chunks.
 */
const buildDatasetChunks = (datasets: Dataset[]): Dataset[][] => {
    const chunks: Dataset[][] = [];
    for (let i = 0; i < datasets.length; i += CHUNK_SIZE) {
        chunks.push(datasets.slice(i, i + CHUNK_SIZE));
    }
    return chunks;
};

/**
 * Validates and adjusts the freshness source type for a dataset, falling back to default if invalid
 */
const validateAndAdjustFreshnessSourceType = (
    freshnessSpec: NonNullable<BulkCreateDatasetAssertionsSpec['freshnessAssertionSpec']>,
    platformUrn: string,
    datasetUrn: string,
): DatasetFreshnessAssertionParametersInput => {
    const { sourceType } = freshnessSpec.evaluationParameters;
    const validSourceTypes = getFreshnessSourceOptions(platformUrn, true);
    const isSourceTypeValid = validSourceTypes.find((option) => option.type === sourceType);

    const adjustedEvaluationParameters = { ...freshnessSpec.evaluationParameters };
    if (!isSourceTypeValid) {
        const defaultSourceType =
            getDefaultFreshnessSourceOption(platformUrn, true) || DatasetFreshnessSourceType.DatahubOperation;
        adjustedEvaluationParameters.sourceType = defaultSourceType;
        console.warn(
            `Invalid source type: ${sourceType} for dataset ${datasetUrn}, using default: ${defaultSourceType}`,
        );
    }

    return adjustedEvaluationParameters;
};

/**
 * Validates and adjusts the volume source type for a dataset, falling back to default if invalid
 */
const validateAndAdjustVolumeSourceType = (
    volumeSpec: NonNullable<BulkCreateDatasetAssertionsSpec['volumeAssertionSpec']>,
    platformUrn: string,
    datasetUrn: string,
    isView: boolean,
): DatasetVolumeAssertionParametersInput => {
    const { sourceType } = volumeSpec.evaluationParameters;
    const validSourceTypes = getVolumeSourceTypeOptions(platformUrn, true, isView);
    const isSourceTypeValid = validSourceTypes.find((option) => option.toLowerCase() === sourceType.toLowerCase());

    const adjustedEvaluationParameters = { ...volumeSpec.evaluationParameters };
    if (!isSourceTypeValid) {
        const defaultSourceType =
            getDefaultVolumeSourceType(platformUrn, true, isView) || DatasetVolumeSourceType.DatahubDatasetProfile;
        adjustedEvaluationParameters.sourceType = defaultSourceType;
        console.warn(
            `Invalid source type: ${sourceType} for dataset ${datasetUrn}, using default: ${defaultSourceType}`,
        );
    }

    return adjustedEvaluationParameters;
};

/**
 * Builds the parameters for upserting a freshness assertion spec for a dataset.
 * Visible for testing.
 * @internal
 * @param dataset - The dataset to upsert the freshness assertion spec for.
 * @param freshnessAssertionSpec - The freshness assertion spec to upsert.
 * @returns The parameters for upserting a freshness assertion spec for a dataset.
 */
export const buildUpsertFreshnessAssertionParams = (
    dataset: Dataset,
    freshnessAssertionSpec: NonNullable<BulkCreateDatasetAssertionsSpec['freshnessAssertionSpec']>,
) => {
    // Validate evaluation parameters work for this dataset, or failover to default
    const freshnessEvaluationParameters = validateAndAdjustFreshnessSourceType(
        freshnessAssertionSpec,
        dataset.platform.urn,
        dataset.urn,
    );
    return {
        variables: {
            input: {
                mode: MonitorMode.Active,
                entityUrn: dataset.urn,
                evaluationParameters: freshnessEvaluationParameters,
                evaluationSchedule: freshnessAssertionSpec.evaluationSchedule,
                schedule:
                    freshnessAssertionSpec.criteria.type === 'MANUAL'
                        ? freshnessAssertionSpec.criteria.schedule
                        : undefined,
                actions: freshnessAssertionSpec.actions,
                inferWithAI: freshnessAssertionSpec.criteria.type === 'AI',
                inferenceSettings:
                    freshnessAssertionSpec.criteria.type === 'AI'
                        ? freshnessAssertionSpec.criteria.inferenceSettings
                        : undefined,
                description: freshnessAssertionSpec.criteria.type === 'AI' ? `Freshness anomaly check` : undefined, // descriptions are automatically generated for manual freshness assertions
            },
        },
    };
};

/**
 * Builds the parameters for upserting a volume assertion spec for a dataset.
 * Visible for testing.
 * @internal
 * @param dataset - The dataset to upsert the volume assertion spec for.
 * @param volumeAssertionSpec - The volume assertion spec to upsert.
 * @returns The parameters for upserting a volume assertion spec for a dataset.
 */
export const buildUpsertVolumeAssertionParams = (
    dataset: Dataset,
    volumeAssertionSpec: NonNullable<BulkCreateDatasetAssertionsSpec['volumeAssertionSpec']>,
) => {
    // Check if the dataset is a view
    const isView = dataset.subTypes?.typeNames?.some((type) => type.toLowerCase() === 'view') ?? false;

    // Validate evaluation parameters work for this dataset, or failover to default
    const volumeEvaluationParameters = validateAndAdjustVolumeSourceType(
        volumeAssertionSpec,
        dataset.platform.urn,
        dataset.urn,
        isView,
    );
    return {
        variables: {
            input: {
                mode: MonitorMode.Active,
                type: VolumeAssertionType.RowCountTotal,
                inferWithAI: true,
                rowCountTotal: {
                    operator: AssertionStdOperator.Between,
                    parameters: {
                        minValue: {
                            type: AssertionStdParameterType.Number,
                            value: '0',
                        },
                        maxValue: {
                            type: AssertionStdParameterType.Number,
                            value: '1000',
                        },
                    },
                },
                description: `Row count volume anomaly check`,
                entityUrn: dataset.urn,
                evaluationParameters: volumeEvaluationParameters,
                evaluationSchedule: volumeAssertionSpec.evaluationSchedule,
                actions: volumeAssertionSpec.actions,
                inferenceSettings: volumeAssertionSpec.inferenceSettings,
            },
        },
    };
};

/**
 * Builds a function that creates assertions for a dataset.
 * Visible for testing.
 * @internal
 * @param upsertDatasetFreshnessAssertion - The mutation function to upsert a freshness assertion.
 * @param upsertDatasetVolumeAssertion - The mutation function to upsert a volume assertion.
 * @param syncSubscription - The mutation function to sync a subscription.
 * @param setProgress - The function to set the progress tracker.
 * @returns A function that creates assertions for a dataset.
 */
export const buildCreateAssertionsForDataset =
    (
        upsertDatasetFreshnessAssertion: UpsertDatasetFreshnessAssertionMonitorMutationFn,
        upsertDatasetVolumeAssertion: UpsertDatasetVolumeAssertionMonitorMutationFn,
        syncSubscription: SyncSubscriptionMutationFn,
        setProgress: (updater: (currentProgress: ProgressTracker) => ProgressTracker) => void,
    ) =>
    async (
        dataset: Dataset,
        freshnessAssertionSpec: BulkCreateDatasetAssertionsSpec['freshnessAssertionSpec'],
        volumeAssertionSpec: BulkCreateDatasetAssertionsSpec['volumeAssertionSpec'],
        subscriptionSpecs: BulkCreateDatasetAssertionsSpec['subscriptionSpecs'],
    ) => {
        const successes: ProgressTracker['successful'] = [];
        const errors: ProgressTracker['errored'] = [];

        // 1. Create the freshness assertion
        if (freshnessAssertionSpec) {
            // Upsert the freshness assertion
            try {
                await upsertDatasetFreshnessAssertion(
                    buildUpsertFreshnessAssertionParams(dataset, freshnessAssertionSpec),
                );
                successes.push({
                    dataset: dataset.urn,
                    type: 'assertion',
                    assertionType: AssertionType.Freshness,
                });
            } catch (error) {
                errors.push({
                    dataset: dataset.urn,
                    type: 'assertion',
                    assertionType: AssertionType.Freshness,
                    error: error instanceof Error ? error.message : 'Unknown error',
                });
            }
        }

        // 2. Create the volume assertion
        if (volumeAssertionSpec) {
            // Upsert the volume assertion
            try {
                await upsertDatasetVolumeAssertion(buildUpsertVolumeAssertionParams(dataset, volumeAssertionSpec));
                successes.push({
                    dataset: dataset.urn,
                    type: 'assertion',
                    assertionType: AssertionType.Volume,
                });
            } catch (error) {
                errors.push({
                    dataset: dataset.urn,
                    type: 'assertion',
                    assertionType: AssertionType.Volume,
                    error: error instanceof Error ? error.message : 'Unknown error',
                });
            }
        }

        // 3. Create the subscriptions
        if (subscriptionSpecs) {
            await Promise.allSettled(
                subscriptionSpecs.map((subscriptionSpec) =>
                    syncSubscription({
                        variables: {
                            input: {
                                entityUrn: dataset.urn,
                                actorUrn: subscriptionSpec.subscriberUrn,
                                entityChangeTypes: subscriptionSpec.entityChangeTypes,
                            },
                        },
                    })
                        .then(() => {
                            successes.push({
                                dataset: dataset.urn,
                                type: 'subscriber',
                                subscriberUrn: subscriptionSpec.subscriberUrn,
                            });
                        })
                        .catch((error) => {
                            errors.push({
                                dataset: dataset.urn,
                                type: 'subscriber',
                                subscriberUrn: subscriptionSpec.subscriberUrn,
                                error: error instanceof Error ? error.message : 'Unknown error',
                            });
                        }),
                ),
            );
        }

        // Update the progress tracker
        setProgress((currentProgress) => ({
            hasFetched: currentProgress.hasFetched,
            total: currentProgress.total,
            completed: currentProgress.completed + 1,
            successful: [...currentProgress.successful, ...successes],
            errored: [...currentProgress.errored, ...errors],
        }));
    };

/**
 * Hook to bulk create dataset assertions.
 * @returns The progress tracker and the bulk create dataset assertions function.
 */
export const useBulkCreateDatasetAssertions = () => {
    const [upsertDatasetFreshnessAssertion] = useUpsertDatasetFreshnessAssertionMonitorMutation();
    const [upsertDatasetVolumeAssertion] = useUpsertDatasetVolumeAssertionMonitorMutation();
    const [syncSubscription] = useSyncSubscriptionMutation();

    const { refetch: refetchSearchResults } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                orFilters: [],
                count: 1,
            },
        },
    });

    const [progress, setProgress] = useState<ProgressTracker>(DEFAULT_PROGRESS_TRACKER);
    const progressRef = useRef<ProgressTracker>(progress);
    progressRef.current = progress;

    const createAssertionsForDataset = buildCreateAssertionsForDataset(
        upsertDatasetFreshnessAssertion,
        upsertDatasetVolumeAssertion,
        syncSubscription,
        setProgress,
    );

    const bulkCreateDatasetAssertions = async (bulkCreateDatasetAssertionsSpec: BulkCreateDatasetAssertionsSpec) => {
        const { assetSelector, freshnessAssertionSpec, volumeAssertionSpec, subscriptionSpecs } =
            bulkCreateDatasetAssertionsSpec;
        const { filters } = assetSelector;
        setProgress({
            hasFetched: false,
            total: 0,
            completed: 0,
            successful: [],
            errored: [],
        });

        // 1. Get the datasets using batched search with URN sorting
        const searchFunction: SearchFunction = async (params) => {
            return (await refetchSearchResults?.(params)) ?? { data: undefined };
        };

        let searchResult;
        try {
            searchResult = await performBatchedSearchWithUrnSorting(searchFunction, {
                filters,
                query: '*',
            });
        } catch (error) {
            const errorMessage =
                error instanceof Error ? error.message : 'Unknown error occurred while searching for datasets.';

            try {
                analytics.event({
                    type: EventType.BulkCreateAssertionSubmissionFailedEvent,
                    surface: 'dataset-health',
                    error: errorMessage,
                });
            } catch (analyticsError) {
                console.error('Error sending bulk create assertion submission failed event', analyticsError);
            }

            // Re-throw the original error with its detailed message so the UI can display it to the user
            throw error instanceof Error ? error : new Error(errorMessage);
        }

        const { datasets, total } = searchResult;

        setProgress({
            hasFetched: true,
            total,
            completed: 0,
            successful: [],
            errored: [],
        });

        try {
            analytics.event({
                type: EventType.BulkCreateAssertionSubmissionEvent,
                surface: 'dataset-health',
                entityCount: total,
                hasFreshnessAssertion: !!freshnessAssertionSpec,
                hasFieldMetricAssertion: false,
                hasVolumeAssertion: !!volumeAssertionSpec,
                hasSubscription: !!subscriptionSpecs?.length,
            });
        } catch (error) {
            console.error('Error sending bulk create assertion submission event', error);
        }

        // Process datasets in chunks to avoid overwhelming the API
        const chunks = buildDatasetChunks(datasets);

        // Process each chunk sequentially using reduce to avoid for...of loop
        await chunks.reduce(async (previousChunk, currentChunk) => {
            await previousChunk; // Wait for the previous chunk to complete
            await Promise.allSettled(
                currentChunk.map(async (dataset) =>
                    createAssertionsForDataset(dataset, freshnessAssertionSpec, volumeAssertionSpec, subscriptionSpecs),
                ),
            );
        }, Promise.resolve());

        // 2. Send the completed event
        try {
            analytics.event({
                type: EventType.BulkCreateAssertionCompletedEvent,
                surface: 'dataset-health',
                entityCount: total,
                failedAssertionCount: progressRef.current.errored.filter((assertion) => assertion.type === 'assertion')
                    .length,
                successAssertionCount: progressRef.current.successful.filter(
                    (assertion) => assertion.type === 'assertion',
                ).length,
                totalAssertionCount: progressRef.current.total,
                hasFreshnessAssertion: !!freshnessAssertionSpec,
                hasVolumeAssertion: !!volumeAssertionSpec,
                hasFieldMetricAssertion: false,
                hasSubscription: !!subscriptionSpecs?.length,
                successSubscriptionCount: progressRef.current.successful.filter(
                    (assertion) => assertion.type === 'subscriber',
                ).length,
                failedSubscriptionCount: progressRef.current.errored.filter(
                    (assertion) => assertion.type === 'subscriber',
                ).length,
            });
        } catch (error) {
            console.error('Error sending bulk create assertion completed event', error);
        }

        // 3. Return the progress tracker
        return progressRef.current;
    };

    return {
        progress,
        bulkCreateDatasetAssertions,
    };
};
