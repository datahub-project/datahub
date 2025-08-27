import { SelectInputMode, ValueTypeId } from '@app/automations/fields/types/values';
import { LogicalPredicate } from '@app/tests/builder/steps/definition/builder/types';

import {
    AssertionActionsInput,
    AssertionAdjustmentSettingsInput,
    AssertionType,
    CronScheduleInput,
    DatasetFreshnessAssertionParametersInput,
    DatasetFreshnessSourceType,
    DatasetVolumeAssertionParametersInput,
    EntityChangeDetailsInput,
    EntityType,
    FabricType,
    FreshnessAssertionSchedule,
} from '@types';

export const INVALID_FRESHNESS_SOURCE_TYPES = [
    DatasetFreshnessSourceType.FieldValue,
    DatasetFreshnessSourceType.FileMetadata,
];

export const PREDICATE_PROPERTIES = [
    {
        id: '_entityType',
        displayName: 'Type',
        description: 'The type of the asset.',
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            mode: SelectInputMode.SINGLE,
            options: [
                {
                    id: 'dataset',
                    displayName: 'Dataset',
                },
            ],
        },
    },
    {
        id: 'platform',
        displayName: 'Platform',
        description: 'The data platform where the asset lives.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.DataPlatform],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: 'container',
        displayName: 'Container',
        description: 'The parent container of the asset.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Container],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: 'domains',
        displayName: 'Domain',
        description: 'The domain that the asset is a part of.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Domain],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'glossaryTerms',
        displayName: 'Glossary Terms',
        description: 'The glossary terms applied to an asset',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.GlossaryTerm],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'tags',
        displayName: 'Tags',
        description: 'The tags applied to an asset',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Tag],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'owners',
        displayName: 'Owned By',
        description: 'The owners of an asset',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.CorpUser, EntityType.CorpGroup],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'origin',
        displayName: 'Environment',
        description: 'The environment of an asset',
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            mode: SelectInputMode.MULTIPLE,
            options: Object.values(FabricType).map((fabricType) => ({
                id: fabricType,
                displayName: fabricType,
            })),
        },
    },
    {
        id: 'urn',
        displayName: 'Asset',
        description: 'The specific asset itself',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Dataset],
            mode: SelectInputMode.MULTIPLE,
        },
    },
];

export type FreshnessCriteria =
    | {
          type: 'AI';
          inferenceSettings: AssertionAdjustmentSettingsInput;
      }
    | {
          type: 'MANUAL';
          schedule: FreshnessAssertionSchedule;
      };

export type BulkCreateDatasetAssertionsSpec = {
    assetSelector: {
        filters: LogicalPredicate;
    };
    freshnessAssertionSpec?: {
        criteria: FreshnessCriteria;
        evaluationParameters: DatasetFreshnessAssertionParametersInput;
        actions?: AssertionActionsInput;
        evaluationSchedule: CronScheduleInput;
    };
    volumeAssertionSpec?: {
        evaluationParameters: DatasetVolumeAssertionParametersInput;
        inferenceSettings: AssertionAdjustmentSettingsInput;
        actions?: AssertionActionsInput;
        evaluationSchedule: CronScheduleInput;
    };
    subscriptionSpecs?: {
        subscriberUrn: string;
        entityChangeTypes: EntityChangeDetailsInput[];
    }[];
};

type ProgressTrackerItemBase = {
    dataset: string;
};

type ProgressTrackerSuccessItem = ProgressTrackerItemBase &
    (
        | {
              type: 'assertion';
              assertionType: AssertionType;
          }
        | {
              type: 'subscriber';
              subscriberUrn: string;
          }
    );

type ProgressTrackerErrorItemBase = ProgressTrackerItemBase & {
    error: string;
};

type ProgressTrackerErroredItem = ProgressTrackerErrorItemBase &
    (
        | {
              type: 'assertion';
              assertionType: AssertionType;
          }
        | {
              type: 'subscriber';
              subscriberUrn: string;
          }
    );

export type ProgressTracker = {
    hasFetched: boolean;
    total: number;
    completed: number;
    successful: ProgressTrackerSuccessItem[];
    errored: ProgressTrackerErroredItem[];
};

export const MAX_BULK_CREATE_DATASET_ASSERTIONS_COUNT = 5000;
