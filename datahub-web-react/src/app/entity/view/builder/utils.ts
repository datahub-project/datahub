import { Entity, EntityType, FacetFilterInput, LogicalOperator } from '../../../../types.generated';
import { ENTITY_FILTER_NAME, UnionType } from '../../../search/utils/constants';

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
