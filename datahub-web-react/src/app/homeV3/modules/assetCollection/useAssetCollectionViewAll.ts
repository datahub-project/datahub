import { useCallback, useMemo } from 'react';
import { useHistory } from 'react-router';

import { ModuleProps } from '@app/homeV3/module/types';
import { ENTITY_FILTER_NAME, ENTITY_SUB_TYPE_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';
import { excludeEmptyAndFilters } from '@app/searchV2/utils/filterUtils';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import { LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { convertLogicalPredicateToOrFilters, isEmptyLogicalPredicate } from '@app/sharedV2/queryBuilder/builder/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { EntityType, FacetFilterInput, FilterOperator } from '@types';

export type ViewAllSearchParams = {
    filters: FacetFilterInput[];
    unionType: UnionType;
};

function convertFilter(
    filter: FacetFilterInput,
    getTypeFromGraphName: (name: string) => EntityType | undefined,
): FacetFilterInput | null {
    // Valueless filters (including `exists`) don't round-trip through the search URL —
    // the serializer drops them — while the tile's query executes them. Never drop one
    // silently: "View all" would show more results than the tile.
    if (!filter.values?.length || filter.condition === FilterOperator.Exists) return null;

    if (filter.field === ENTITY_FILTER_NAME) {
        // The search page's generateOrFilters rebuilds entity-type filters from the URL
        // without their condition/negated flags, so only plain equality survives.
        if (filter.negated || (filter.condition && filter.condition !== FilterOperator.Equal)) return null;
        const entityTypes = filter.values.map((value) => getTypeFromGraphName(value));
        if (entityTypes.some((type) => type === undefined)) return null;
        // Target the combined type/subtype field so the search page renders its native "Type" chip.
        return { field: ENTITY_SUB_TYPE_FILTER_NAME, values: entityTypes as string[], condition: filter.condition };
    }

    return { field: filter.field, values: filter.values, condition: filter.condition, negated: filter.negated };
}

/**
 * Converts a collection's dynamic filter predicate into search-page URL params.
 *
 * The predicate is first converted through the exact pipeline the collection tile
 * uses to run its own query (convertLogicalPredicateToOrFilters + excludeEmptyAndFilters),
 * so the button's target and the tile's contents cannot drift apart. The resulting
 * orFilters are then mapped onto the search URL's flat format, which can express
 * exactly two shapes: a single AND group, or an OR of single-filter groups.
 *
 * Returns null when the orFilters have no faithful URL representation (deeper
 * nesting, exists/valueless conditions, non-equality entity-type conditions,
 * unmappable entity types) — callers should hide the "View all" affordance
 * rather than show results that differ from the tile.
 */
export function convertLogicalPredicateToViewAllParams(
    predicate: LogicalPredicate,
    getTypeFromGraphName: (name: string) => EntityType | undefined,
): ViewAllSearchParams | null {
    let orFilters;
    try {
        orFilters = excludeEmptyAndFilters(convertLogicalPredicateToOrFilters(predicate));
    } catch (e) {
        // Unknown operators throw inside the shared converter.
        return null;
    }
    if (!orFilters?.length) return null;

    let unionType: UnionType;
    let rawFilters: FacetFilterInput[];
    if (orFilters.length === 1) {
        unionType = UnionType.AND;
        rawFilters = orFilters[0].and ?? [];
    } else if (orFilters.every((group) => group.and?.length === 1)) {
        unionType = UnionType.OR;
        rawFilters = orFilters.map((group) => group.and![0]);
    } else {
        // OR of multi-filter groups — the flat URL format cannot express it.
        return null;
    }

    const filters: FacetFilterInput[] = [];
    const ok = rawFilters.every((rawFilter) => {
        const filter = convertFilter(rawFilter, getTypeFromGraphName);
        if (filter === null) return false;
        filters.push(filter);
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
 * Whether a (draft) dynamic filter supports the "View all" affordance. Used by the
 * collection builder to hint authors when their filter won't get the button.
 * Predicates with no effective conditions (empty, or blank rows only) count as
 * supported — there is nothing to warn about yet.
 */
export function isViewAllSupported(
    predicate: LogicalPredicate | null | undefined,
    getTypeFromGraphName: (name: string) => EntityType | undefined,
): boolean {
    if (!predicate || isEmptyLogicalPredicate(predicate)) return true;
    if (convertLogicalPredicateToViewAllParams(predicate, getTypeFromGraphName) !== null) return true;
    try {
        const orFilters = excludeEmptyAndFilters(convertLogicalPredicateToOrFilters(predicate));
        return !orFilters?.length;
    } catch (e) {
        return false;
    }
}

/**
 * Returns a "View all" navigation callback for dynamic-filter collections,
 * or undefined when the collection's contents can't be faithfully reproduced
 * on the search page (manual urn lists, unrepresentable predicates) —
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
