import { ENTITY_FILTER_NAME, ENTITY_SUB_TYPE_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';
import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { isLogicalPredicate, mapOperator } from '@app/sharedV2/queryBuilder/builder/utils';

import { EntityType, FacetFilterInput, FilterOperator } from '@types';

export type ViewAllSearchParams = {
    filters: FacetFilterInput[];
    unionType: UnionType;
};

const OPERATOR_TO_UNION_TYPE: Partial<Record<LogicalOperatorType, UnionType>> = {
    [LogicalOperatorType.AND]: UnionType.AND,
    [LogicalOperatorType.OR]: UnionType.OR,
};

function convertLeaf(
    leaf: PropertyPredicate,
    getTypeFromGraphName: (name: string) => EntityType | undefined,
): FacetFilterInput | null | undefined {
    // Blank builder row — skip (undefined), don't fail the conversion.
    if (!leaf.property) return undefined;

    // Check the operator BEFORE the values guard: an `exists` leaf has no values, and
    // skipping it would make "View all" show more than the tile. It must abort instead.
    let condition: FilterOperator | undefined;
    if (leaf.operator) {
        try {
            condition = mapOperator(leaf.operator);
        } catch (e) {
            return null;
        }
        // Valueless conditions don't round-trip through the search URL.
        if (condition === FilterOperator.Exists) return null;
    }

    // A property with no values constrains nothing — skip it.
    if (!leaf.values?.length) return undefined;

    if (leaf.property === ENTITY_FILTER_NAME) {
        const entityTypes = leaf.values.map((value) => getTypeFromGraphName(value));
        if (entityTypes.some((type) => type === undefined)) return null;
        // Target the combined type/subtype field so the search page renders its native "Type" chip.
        return { field: ENTITY_SUB_TYPE_FILTER_NAME, values: entityTypes as string[], condition };
    }

    return { field: leaf.property, values: leaf.values, condition };
}

/**
 * Converts a collection's dynamic filter predicate into search-page URL params.
 * Returns null when the predicate is not exactly representable in the URL
 * (nested groups, NOT, exists, unknown operators, unmappable entity types) —
 * callers should hide the "View all" affordance rather than show wrong results.
 */
export function convertLogicalPredicateToViewAllParams(
    predicate: LogicalPredicate,
    getTypeFromGraphName: (name: string) => EntityType | undefined,
): ViewAllSearchParams | null {
    if (predicate?.type !== 'logical') return null;

    const unionType = OPERATOR_TO_UNION_TYPE[predicate.operator];
    if (unionType === undefined) return null;

    if (predicate.operands.some((operand) => isLogicalPredicate(operand))) return null;

    const operands = predicate.operands as PropertyPredicate[];

    let filters: FacetFilterInput[] | null = [];
    operands.forEach((leaf) => {
        if (filters === null) return;
        const filter = convertLeaf(leaf, getTypeFromGraphName);
        if (filter === null) {
            filters = null;
        } else if (filter) {
            (filters as FacetFilterInput[]).push(filter);
        }
    });

    if (filters === null || !filters.length) return null;

    return { filters, unionType };
}
