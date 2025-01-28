import {
    DataHubViewType,
    Entity,
    EntityType,
    FacetFilter,
    FacetFilterInput,
    LogicalOperator,
} from '../../../../types.generated';
import {
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    UnionType,
    FILTER_DELIMITER,
    TYPE_NAMES_FILTER_NAME,
} from '../../../search/utils/constants';

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
export const buildViewDefinition = (filters: Array<FacetFilterInput>, operatorType: LogicalOperator) => {
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
        definition: buildViewDefinition(filters, operatorType),
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
 */
export const buildEntityCache = (entities: Entity[]) => {
    const cache = new Map();
    entities.forEach((entity) => cache.set(entity.urn, entity));
    return cache;
};

/**
 * Returns 'true' if any urns are not present in an entity cache, 'false' otherwise.
 */
export const isResolutionRequired = (urns: string[], cache: Map<string, Entity>) => {
    const uncachedUrns = urns.filter((urn) => !cache.has(urn));
    return uncachedUrns.length > 0;
};
