import { AggregationGroup } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { OperationType } from '@src/types.generated';

export const AVAILABLE_OPERATION_TYPES = [
    OperationType.Insert,
    OperationType.Update,
    OperationType.Delete,
    OperationType.Alter,
    OperationType.Create,
    OperationType.Drop,
];

export const DEFAULT_OPERATION_TYPES = AVAILABLE_OPERATION_TYPES;

export const CUSTOM_KEY_PREFIX = 'custom_';

export const DEFAULT_COLOR = '#EBECF0';

export const AGGREGATION_GROUP_TO_COLORS_MAPPING = {
    // purple colors
    [AggregationGroup.Purple]: ['#CAC3F1', '#705EE4', '#3E2F9D'],
    // red colors
    [AggregationGroup.Red]: ['#F2998D', '#BF4636', '#A32C1C'],
};

export const AGGREGATION_GROUPS = Object.values(AggregationGroup);
