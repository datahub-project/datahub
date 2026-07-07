import { useCallback, useMemo } from 'react';
import { useHistory } from 'react-router';

import { ModuleProps } from '@app/homeV3/module/types';
import { ENTITY_FILTER_NAME, ENTITY_SUB_TYPE_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { isLogicalPredicate, mapOperator } from '@app/sharedV2/queryBuilder/builder/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

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

    // The tile's backend query turns a valueless property into a match-nothing terms
    // query; skipping it here would make "View all" show MORE than the tile. Abort.
    if (!leaf.values?.length) return null;

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

    const filters: FacetFilterInput[] = [];
    const ok = operands.every((leaf) => {
        const filter = convertLeaf(leaf, getTypeFromGraphName);
        if (filter === null) return false;
        if (filter) filters.push(filter);
        return true;
    });

    if (!ok || !filters.length) return null;

    // The search page's generateOrFilters fans each entity-type filter's values into
    // separate OR'd and-clauses, so AND [Type=A, Type=B] would become A-OR-B there —
    // but the tile's query ANDs both into one clause (an entity can't be two types),
    // so it's zero results. Only allowed under OR, where the semantics stay equivalent.
    if (unionType === UnionType.AND) {
        const entitySubTypeFilterCount = filters.filter(
            (filter) => filter.field === ENTITY_SUB_TYPE_FILTER_NAME,
        ).length;
        if (entitySubTypeFilterCount > 1) return null;
    }

    return { filters, unionType };
}

/**
 * Returns a "View all" navigation callback for dynamic-filter collections,
 * or undefined when the collection's contents can't be faithfully reproduced
 * on the search page (manual urn lists, nested/unsupported predicates) —
 * in which case the button is hidden.
 */
export default function useAssetCollectionViewAll(module: ModuleProps['module']): (() => void) | undefined {
    const history = useHistory();
    const entityRegistry = useEntityRegistryV2();
    const params = module.properties.params.assetCollectionParams;

    const searchParams = useMemo(() => {
        // Manual collections are hand-ordered urn lists; the search page can't reproduce them faithfully.
        if (params?.assetUrns?.length) return null;
        if (!params?.dynamicFilterJson) return null;
        try {
            const predicate = JSON.parse(params.dynamicFilterJson);
            return convertLogicalPredicateToViewAllParams(predicate, (name) =>
                entityRegistry.getTypeFromGraphName(name),
            );
        } catch (e) {
            return null;
        }
    }, [params?.assetUrns, params?.dynamicFilterJson, entityRegistry]);

    const onClickViewAll = useCallback(() => {
        if (!searchParams) return;
        navigateToSearchUrl({
            history,
            query: '*',
            filters: searchParams.filters,
            unionType: searchParams.unionType,
        });
    }, [searchParams, history]);

    return searchParams ? onClickViewAll : undefined;
}
