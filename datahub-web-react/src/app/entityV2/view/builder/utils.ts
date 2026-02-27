import { BUILD_FILTERS_TAB_KEY, SELECT_ASSETS_TAB_KEY, URN_FILTER_NAME } from '@app/entityV2/view/builder/constants';
import { ViewFilter } from '@app/entityV2/view/builder/types';
import { ViewBuilderState } from '@app/entityV2/view/types';
import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';

import { FacetFilter, FilterOperator, LogicalOperator } from '@types';

/** Non-nullable shorthand for the definition property of ViewBuilderState. */
type ViewDefinition = NonNullable<ViewBuilderState['definition']>;

/**
 * Maps a UI operator ID (from PropertyPredicate.operator) to the backend FilterOperator.
 * Falls back to FilterOperator.Equal for unknown operators.
 */
export function mapUiOperatorToCondition(operator: string | undefined): FilterOperator | undefined {
    if (!operator) return undefined;
    const map: Record<string, FilterOperator> = {
        equals: FilterOperator.Equal,
        exists: FilterOperator.Exists,
        contains_str: FilterOperator.Contain,
        contains_any: FilterOperator.Contain,
        starts_with: FilterOperator.StartWith,
        regex_match: FilterOperator.Contain,
        greater_than: FilterOperator.GreaterThan,
        less_than: FilterOperator.LessThan,
        is_true: FilterOperator.Equal,
        is_false: FilterOperator.Equal,
    };
    return map[operator] ?? FilterOperator.Equal;
}

/**
 * Maps a backend FilterOperator back to a UI operator ID string.
 * For EQUAL with boolean values ("true"/"false"), returns the is_true/is_false
 * operator so the query builder renders the correct unary toggle.
 * Falls back to 'equals' for unknown conditions.
 */
export function mapConditionToUiOperator(condition: FilterOperator | null | undefined, values?: string[]): string {
    if (!condition) return 'equals';

    if (condition === FilterOperator.Equal && values?.length === 1) {
        if (values[0] === 'true') return 'is_true';
        if (values[0] === 'false') return 'is_false';
    }

    const map: Record<string, string> = {
        [FilterOperator.Equal]: 'equals',
        [FilterOperator.Exists]: 'exists',
        [FilterOperator.Contain]: 'contains_str',
        [FilterOperator.StartWith]: 'starts_with',
        [FilterOperator.GreaterThan]: 'greater_than',
        [FilterOperator.LessThan]: 'less_than',
    };
    return map[condition] ?? 'equals';
}

/**
 * Converts selected asset URNs into a single "urn" filter.
 * All assets are stored under the "urn" field so the View shows exactly
 * those entities — it does NOT use entity-type-specific filter fields.
 */
export function selectedUrnsToFilters(selectedUrns: string[]): ViewFilter[] {
    if (selectedUrns.length === 0) return [];
    return [{ field: URN_FILTER_NAME, values: selectedUrns }];
}

/**
 * Resolves the filter values for a PropertyPredicate.
 * Boolean operators (is_true/is_false) are unary — the UI provides no value
 * input, so we inject "true"/"false" as the value to send to the backend.
 */
function resolveFilterValues(prop: PropertyPredicate): string[] {
    if (prop.operator === 'is_true') return ['true'];
    if (prop.operator === 'is_false') return ['false'];
    return prop.values || [];
}

/**
 * Recursively extracts all PropertyPredicates from a LogicalPredicate tree,
 * flattening nested groups into a single-level list of ViewFilters.
 * Propagates negation from NOT groups down to each filter.
 */
function flattenPredicateToFilters(predicate: LogicalPredicate | PropertyPredicate, isNegated: boolean): ViewFilter[] {
    if (predicate.type === 'property') {
        const prop = predicate as PropertyPredicate;
        if (!prop.property) return [];
        return [
            {
                field: prop.property,
                values: resolveFilterValues(prop),
                condition: mapUiOperatorToCondition(prop.operator),
                negated: isNegated || undefined,
            },
        ];
    }

    const logical = predicate as LogicalPredicate;
    const childNegated = logical.operator === LogicalOperatorType.NOT ? !isNegated : isNegated;

    return (logical.operands || []).flatMap((op) => flattenPredicateToFilters(op, childNegated));
}

/**
 * Converts a LogicalPredicate from the query builder into flat filter objects.
 * Recursively flattens nested groups and preserves each condition's operator
 * as the ViewFilter.condition field.
 */
export function logicalPredicateToFilters(predicate: LogicalPredicate | null | undefined): {
    operator: LogicalOperator;
    filters: ViewFilter[];
} {
    if (!predicate || !predicate.operands?.length) {
        return { operator: LogicalOperator.And, filters: [] };
    }

    const operator = predicate.operator === LogicalOperatorType.OR ? LogicalOperator.Or : LogicalOperator.And;
    const isNegated = predicate.operator === LogicalOperatorType.NOT;
    const filters = (predicate.operands || []).flatMap((op) => flattenPredicateToFilters(op, isNegated));

    return { operator, filters };
}

/**
 * Extracts selected asset URNs from saved view filters.
 * Only reads URNs from the "urn" filter field (produced by the Select Assets tab).
 */
export function filtersToSelectedUrns(filters: ViewFilter[]): string[] {
    const urnFilter = filters.find((f) => f.field === URN_FILTER_NAME);
    return urnFilter?.values ?? [];
}

/**
 * Converts existing ViewBuilderState filters back to a LogicalPredicate
 * for the Build Filters tab. Restores each filter's condition back to
 * the UI operator, and wraps in NOT if all filters are negated.
 */
export function filtersToLogicalPredicate(
    operator: LogicalOperator | undefined,
    filters: ViewFilter[],
): LogicalPredicate {
    const allNegated = filters.length > 0 && filters.every((f) => f.negated);

    const operands: PropertyPredicate[] = filters.map((filter) => {
        const uiOperator = mapConditionToUiOperator(filter.condition, filter.values);
        const isBooleanOp = uiOperator === 'is_true' || uiOperator === 'is_false';
        return {
            type: 'property' as const,
            property: filter.field,
            operator: uiOperator,
            values: isBooleanOp ? [] : filter.values || [],
        };
    });

    if (allNegated) {
        return {
            type: 'logical',
            operator: LogicalOperatorType.NOT,
            operands,
        };
    }

    const logicalOperator = operator === LogicalOperator.Or ? LogicalOperatorType.OR : LogicalOperatorType.AND;

    return {
        type: 'logical',
        operator: logicalOperator,
        operands,
    };
}

/**
 * Determines which tab should be active based on saved filter state.
 *
 * Heuristic:
 * 1. Empty filters → Build Filters (default for new views).
 * 2. Any filter uses the "urn" field → Select Assets (only that tab produces it).
 * 3. Otherwise → Build Filters.
 */
export function getInitialTabKey(filters: ViewFilter[]): string {
    const hasUrnField = filters.some((f) => f.field === URN_FILTER_NAME);
    if (hasUrnField) {
        return SELECT_ASSETS_TAB_KEY;
    }
    return BUILD_FILTERS_TAB_KEY;
}

/**
 * Builds a view definition object compatible with ViewBuilderState.
 * ViewFilter is structurally compatible with FacetFilter; the cast bridges
 * the generated __typename field that ViewFilter intentionally omits.
 */
export function buildViewDefinition(operator: LogicalOperator, filters: ViewFilter[]): ViewDefinition {
    return {
        entityTypes: [],
        filter: {
            operator,
            filters: filters as FacetFilter[],
        },
    };
}

/**
 * Builds a lookup map from entity URN to entity object.
 * Shared by ViewDefinitionBuilder and SelectedFilterValues to avoid duplication.
 */
export function buildEntityMap<T extends { urn: string }>(entities: T[]): Record<string, T> {
    const map: Record<string, T> = {};
    entities.forEach((entity) => {
        map[entity.urn] = entity;
    });
    return map;
}

// Re-export V1 utils that other files import from this path
export { convertNestedSubTypeFilter, buildEntityCache, isResolutionRequired } from '@app/entity/view/builder/utils';
