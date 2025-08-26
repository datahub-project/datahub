import {
    AGGREGATION_GROUPS,
    AGGREGATION_GROUP_TO_COLORS_MAPPING,
    CUSTOM_KEY_PREFIX,
    DEFAULT_COLOR,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/constants';
import {
    AggregationGroup,
    AnyOperationType,
    CustomOperationType,
    CustomOperations,
    OperationsData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';
import { getColorAccessor } from '@src/alchemy-components/components/CalendarChart/utils';
import { OperationType, OperationsAggregationsResult } from '@src/types.generated';

export function getCustomOperationsFromAggregations(
    aggregations: OperationsAggregationsResult | undefined | null,
    defaultCustomOperationTypes?: CustomOperationType[],
): CustomOperationType[] {
    const customOperationTypesFromDatum = (aggregations?.customOperationsMap || []).map((value) => value.key);
    const customOperationTypes = [...customOperationTypesFromDatum, ...(defaultCustomOperationTypes || [])];
    return customOperationTypes;
}

export function convertAggregationsToOperationsData(
    aggregations: OperationsAggregationsResult | undefined | null,
    defaultCustomOperationTypes?: CustomOperationType[],
): OperationsData {
    const inserts = aggregations?.totalInserts ?? 0;
    const updates = aggregations?.totalUpdates ?? 0;
    const deletes = aggregations?.totalDeletes ?? 0;
    const alters = aggregations?.totalAlters ?? 0;
    const creates = aggregations?.totalCreates ?? 0;
    const drops = aggregations?.totalDrops ?? 0;
    const totalCustoms = aggregations?.totalCustoms ?? 0;
    const totalOperations = inserts + updates + deletes + alters + creates + drops + totalCustoms;

    const customOperationTypes = getCustomOperationsFromAggregations(aggregations, defaultCustomOperationTypes);

    const prefixedCustomOperations: CustomOperations = customOperationTypes.reduce((accumulatedOperations, key) => {
        // Add prefixes to custom operation types in order to workaround keys overlap with predefined operation types
        const customKey = addPrefix(key);
        return {
            ...accumulatedOperations,
            [customKey]: {
                value: aggregations?.customOperationsMap?.find((value) => value.key === key)?.value ?? 0,
                group: AggregationGroup.Purple,
                type: OperationType.Custom,
                customType: key,
                name: key,
                key: customKey,
            },
        };
    }, {});

    return {
        summary: {
            // We don't support MoM right now
            mom: null,
            totalOperations,
            totalCustomOperations: totalCustoms,
        },
        operations: {
            inserts: {
                value: inserts,
                key: OperationType.Insert,
                group: AggregationGroup.Purple,
                type: OperationType.Insert,
                name: 'Insert',
            },
            updates: {
                value: updates,
                key: OperationType.Update,
                group: AggregationGroup.Purple,
                type: OperationType.Update,
                name: 'Update',
            },
            deletes: {
                value: deletes,
                key: OperationType.Delete,
                group: AggregationGroup.Red,
                type: OperationType.Delete,
                name: 'Delete',
            },
            alters: {
                value: alters,
                key: OperationType.Alter,
                group: AggregationGroup.Purple,
                type: OperationType.Alter,
                name: 'Alter',
            },
            creates: {
                value: creates,
                key: OperationType.Create,
                group: AggregationGroup.Purple,
                type: OperationType.Create,
                name: 'Create',
            },
            drops: {
                value: drops,
                key: OperationType.Drop,
                group: AggregationGroup.Red,
                type: OperationType.Drop,
                name: 'Drop',
            },
            ...prefixedCustomOperations,
        },
    };
}

export function getSumOfOperationsByAggregationGroup(
    operationsData: OperationsData,
    aggregationGroup: AggregationGroup,
    operationTypes: (OperationType | CustomOperationType)[],
) {
    return (
        Object.entries(operationsData.operations)
            .map(([_, valueOfOperation]) => valueOfOperation)
            .filter((valueOfOperation) => valueOfOperation.group === aggregationGroup)
            // FYI: filter by value.type for predefined operation types
            // filter by prefixed value.customType for custom operation types
            .filter(
                (valueOfOperation) =>
                    operationTypes.includes(valueOfOperation.type) ||
                    (valueOfOperation.type === OperationType.Custom &&
                        valueOfOperation.customType &&
                        operationTypes?.includes?.(addPrefix(valueOfOperation.customType))),
            )
            .reduce((sumOfValues, valueOfOperation) => sumOfValues + valueOfOperation.value, 0)
    );
}

export function createColorAccessors(
    summary: OperationsData | undefined,
    data: CalendarData<OperationsData>[],
    types: AnyOperationType[],
) {
    // Calculate max values for each group
    const maxValueMapping = AGGREGATION_GROUPS.reduce(
        (mapping, group) => ({
            ...mapping,
            [group]: Math.max(
                ...data.map((datum) => getSumOfOperationsByAggregationGroup(datum.value, AggregationGroup.Red, types)),
                0,
            ),
        }),
        {},
    );

    // Color accessor for square (day)
    const colorAccessor = getColorAccessor<OperationsData>(
        data,
        AGGREGATION_GROUPS.reduce(
            (mapping, group) => ({
                ...mapping,
                [group]: {
                    valueAccessor: (value: OperationsData) => getSumOfOperationsByAggregationGroup(value, group, types),
                    colors: AGGREGATION_GROUP_TO_COLORS_MAPPING[group],
                },
            }),
            {},
        ),
        DEFAULT_COLOR,
    );

    // Color accessors for each operation
    const changeTypesAccessors = Object.entries(summary?.operations ?? {}).reduce((accessors, [key, value]) => {
        const colors = AGGREGATION_GROUP_TO_COLORS_MAPPING[value.group];
        const maxValue = maxValueMapping[value.group];

        return {
            ...accessors,
            [value.key]: getColorAccessor<OperationsData>(
                data,
                {
                    [value.key]: {
                        valueAccessor: (datum) => datum.operations?.[key]?.value ?? 0,
                        colors,
                    },
                },
                colors[0],
                maxValue,
            ),
        };
    }, {});

    return {
        day: colorAccessor,
        ...changeTypesAccessors,
    };
}

export function addPrefix(value: string, prefix = CUSTOM_KEY_PREFIX): string {
    return `${prefix}${value}`;
}

export function hasPrefix(value: string, prefix = CUSTOM_KEY_PREFIX): boolean {
    return value.startsWith(prefix);
}

export function removePrefix(value: string, prefix = CUSTOM_KEY_PREFIX): string {
    if (hasPrefix(value, prefix)) return value.slice(prefix.length);
    return value;
}
