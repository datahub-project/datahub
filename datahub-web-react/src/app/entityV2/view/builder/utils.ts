import { BUILD_FILTERS_TAB_KEY, SELECT_ASSETS_TAB_KEY, URN_FILTER_NAME } from '@app/entityV2/view/builder/constants';
import { ViewFilter } from '@app/entityV2/view/builder/types';
import { ViewBuilderState } from '@app/entityV2/view/types';
import { parseJsonToLogicalPredicate } from '@app/entityV2/view/utils';
import {
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    FILTER_DELIMITER,
    TYPE_NAMES_FILTER_NAME,
    UnionType,
} from '@app/search/utils/constants';
import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';

import {
    DataHubViewType,
    Entity,
    EntityType,
    FacetFilter,
    FacetFilterInput,
    FilterOperator,
    LogicalOperator,
} from '@types';

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
        within: FilterOperator.DescendantsIncl,
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
        [FilterOperator.DescendantsIncl]: 'within',
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
export function resolveFilterValues(prop: PropertyPredicate): string[] {
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
 * Extracts selected asset URNs from a LogicalPredicate by recursively searching
 * for the "urn" property. Used when restoring from json field.
 */
export function extractUrnsFromLogicalPredicate(
    predicate: LogicalPredicate | PropertyPredicate | null | undefined,
): string[] {
    if (!predicate) return [];

    if (predicate.type === 'property') {
        const prop = predicate as PropertyPredicate;
        if (prop.property === URN_FILTER_NAME) {
            return prop.values ?? [];
        }
        return [];
    }

    const logical = predicate as LogicalPredicate;
    const foundUrns = (logical.operands ?? [])
        .map((operand) => extractUrnsFromLogicalPredicate(operand))
        .find((urns) => urns.length > 0);
    return foundUrns ?? [];
}

/**
 * Extracts selected asset URNs from saved view filters.
 * Only reads URNs from the "urn" filter field (produced by the Select Assets tab).
 * Falls back to searching json structure if filters array is empty.
 */
export function filtersToSelectedUrns(filters: ViewFilter[], json?: string | null): string[] {
    // First try legacy filters array
    const urnFilter = filters.find((f) => f.field === URN_FILTER_NAME);
    if (urnFilter?.values?.length) {
        return urnFilter.values;
    }

    // Fallback: extract from json LogicalPredicate structure
    if (json) {
        const logicalPredicate = parseJsonToLogicalPredicate(json);
        return extractUrnsFromLogicalPredicate(logicalPredicate);
    }

    return [];
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

    const operands: (PropertyPredicate | LogicalPredicate)[] = filters.map((filter) => {
        const uiOperator = mapConditionToUiOperator(filter.condition, filter.values);
        const isBooleanOp = uiOperator === 'is_true' || uiOperator === 'is_false';
        const propertyPredicate: PropertyPredicate = {
            type: 'property' as const,
            property: filter.field,
            operator: uiOperator,
            values: isBooleanOp ? [] : filter.values || [],
        };
        if (filter.negated && !allNegated) {
            return {
                type: 'logical' as const,
                operator: LogicalOperatorType.NOT,
                operands: [propertyPredicate],
            };
        }
        return propertyPredicate;
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
 * Falls back to json field if filters array is empty (for new views with json).
 */
export function getInitialTabKey(filters: ViewFilter[], json?: string | null): string {
    // First check legacy filters array
    const hasUrnField = filters.some((f) => f.field === URN_FILTER_NAME);
    if (hasUrnField) {
        return SELECT_ASSETS_TAB_KEY;
    }

    // Fallback: check json structure for urn property
    if (!hasUrnField && json) {
        const logicalPredicate = parseJsonToLogicalPredicate(json);
        const urns = extractUrnsFromLogicalPredicate(logicalPredicate);
        if (urns.length > 0) {
            return SELECT_ASSETS_TAB_KEY;
        }
    }

    return BUILD_FILTERS_TAB_KEY;
}

/**
 * Builds a view definition object compatible with ViewBuilderState.
 * A View's entity-type scope lives in its own top-level `entityTypes` field rather
 * than in the filter list, and this builder edits filters only — so the caller's
 * existing scope is threaded through unchanged instead of being rebuilt here.
 * ViewFilter is structurally compatible with FacetFilter; the cast bridges
 * the generated __typename field that ViewFilter intentionally omits.
 */
export function buildViewDefinition(
    operator: LogicalOperator,
    filters: ViewFilter[],
    entityTypes: EntityType[],
    logicalPredicate?: LogicalPredicate | null,
): ViewDefinition {
    return {
        entityTypes,
        filter: {
            operator,
            filters: filters as FacetFilter[],
        },
        ...(logicalPredicate && { logicalPredicate }),
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

/**
 * Extract the special "Entity Type" filter values from a list
 * of filters.
 */
export const extractEntityTypesFilterValues = (filters: Array<FacetFilterInput>) => {
    // Currently we only support 1 entity type filter.
    return filters
        .filter((filter) => filter.field === ENTITY_FILTER_NAME)
        .flatMap((filter) => filter.values as EntityType[]);
};

/**
 * Converts the nested subtype filter to be split into entity type filters and subType filters.
 * Right now we don't allow mixing of entity type and subType filters when creating a view since
 * this filter requires an OR between entity type and subType but AND between other filter types (like tags).
 * Example: { field: "_entityType␞typeNames" values: ["DATASETS␞table"] } -> { field: "typeNames", values: ["table"]}
 * Example: { field: "_entityType␞typeNames" values: ["DATASETS", "CONTAINERS"] } -> { field: "_entityType", values: ["DATASETS", "CONTAINERS"]}
 */
export function convertNestedSubTypeFilter(filters: Array<FacetFilterInput>) {
    const convertedFilters = filters.filter((f) => f.field !== ENTITY_SUB_TYPE_FILTER_NAME) || [];
    const nestedSubTypeFilter = filters.find((f) => f.field === ENTITY_SUB_TYPE_FILTER_NAME);
    if (nestedSubTypeFilter) {
        const entityTypeFilterValues: string[] = [];
        const subTypeFilterValues: string[] = [];
        nestedSubTypeFilter.values?.forEach((value) => {
            if (!value.includes(FILTER_DELIMITER)) {
                entityTypeFilterValues.push(value);
            } else {
                const nestedValues = value.split(FILTER_DELIMITER);
                subTypeFilterValues.push(nestedValues[nestedValues.length - 1]);
            }
        });
        if (entityTypeFilterValues.length) {
            convertedFilters.push({ field: ENTITY_FILTER_NAME, values: entityTypeFilterValues });
        }
        if (subTypeFilterValues.length) {
            convertedFilters.push({ field: TYPE_NAMES_FILTER_NAME, values: subTypeFilterValues });
        }
    }
    return convertedFilters;
}

/**
 * Build an object representation of a View Definition, which consists of a list of entity types +
 * a set of filters joined in either conjunction or disjunction.
 *
 * @param filters a list of Facet Filter Inputs representing the view filters. This can include the entity type filter.
 * @param operatorType a logical operator to be used when joining the filters into the View definition.
 */
const buildFacetFilterViewDefinition = (filters: Array<FacetFilterInput>, operatorType: LogicalOperator) => {
    const convertedFilters = convertNestedSubTypeFilter(filters);
    const entityTypes = extractEntityTypesFilterValues(convertedFilters);
    const filteredFilters = convertedFilters.filter((filter) => filter.field !== ENTITY_FILTER_NAME);
    return {
        entityTypes,
        filter: {
            operator: operatorType,
            filters: (filteredFilters.length > 0 ? filteredFilters : []) as FacetFilter[],
        },
    };
};

/**
 * Build an object representation of a View Definition, which consists of a list of entity types +
 * a set of filters joined in either conjunction or disjunction.
 *
 * @param filters a list of Facet Filter Inputs representing the view filters. This can include the entity type filter.
 * @param operatorType a logical operator to be used when joining the filters into the View definition.
 */
export const buildInitialViewState = (filters: Array<FacetFilterInput>, operatorType: LogicalOperator) => {
    return {
        viewType: DataHubViewType.Personal,
        name: '',
        description: null,
        definition: buildFacetFilterViewDefinition(filters, operatorType),
    };
};

/**
 * Convert a LogicalOperator to the equivalent UnionType.
 */
export const toUnionType = (operator: LogicalOperator) => {
    if (operator === LogicalOperator.And) {
        return UnionType.AND;
    }
    return UnionType.OR;
};

/**
 * Convert a UnionType to the equivalent LogicalOperator.
 */
export const fromUnionType = (unionType: UnionType) => {
    if (unionType === 0) {
        return LogicalOperator.And;
    }
    return LogicalOperator.Or;
};

/**
 * Returns a map of entity urn to entity from a list of entities.
 * Skips null slots from `entities(urns:)` when a requested entity is missing.
 *
 * @param entities - Batch `getEntities` payload, which may include nulls.
 * @returns Map of URN to resolved entity.
 */
export function buildEntityCache(entities: Array<Entity | null | undefined> | null | undefined): Map<string, Entity> {
    const cache = new Map<string, Entity>();
    (entities ?? []).forEach((entity) => {
        if (entity?.urn) {
            cache.set(entity.urn, entity);
        }
    });
    return cache;
}

/**
 * Returns true if any urns are not present in an entity cache.
 * `attemptedUrns` are URNs already requested so a null response does not refetch forever.
 *
 * @param urns - URNs to hydrate.
 * @param cache - Resolved entities keyed by URN.
 * @param attemptedUrns - URNs already sent to `getEntities`.
 * @returns True when at least one URN still needs a fetch.
 */
export function isResolutionRequired(
    urns: string[],
    cache: Map<string, Entity>,
    attemptedUrns: ReadonlySet<string> = new Set(),
): boolean {
    return urns.some((urn) => !cache.has(urn) && !attemptedUrns.has(urn));
}
