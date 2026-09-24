import { AggregationGroup } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { OperationType } from '@src/types.generated';

const AVAILABLE_OPERATION_TYPES = [
    OperationType.Insert,
    OperationType.Update,
    OperationType.Delete,
    OperationType.Alter,
    OperationType.Create,
    OperationType.Drop,
];

export const DEFAULT_OPERATION_TYPES = AVAILABLE_OPERATION_TYPES;

export const CUSTOM_KEY_PREFIX = 'custom_';

export const AGGREGATION_GROUPS = Object.values(AggregationGroup);
