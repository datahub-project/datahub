import { useMemo } from 'react';

import { useEntityRegistry } from '@app/useEntityRegistry';
import { SelectOption } from '@src/alchemy-components/components/Select/types';

import { DataProduct, EntityType } from '@types';

export type DataProductTreeOption = SelectOption & {
    entity?: DataProduct;
    depth?: number;
    ancestorUrns?: string[];
    /** Might have children that haven't been fetched yet — show caret. */
    isEmptyNode?: boolean;
    isLoadingPlaceholder?: boolean;
};

type BuildArgs = {
    entities: DataProduct[];
    excludeSet: ReadonlySet<string>;
    loadingSet: ReadonlySet<string>;
    getDisplayName: (entity: DataProduct) => string;
};

const EMPTY_URN_SET: ReadonlySet<string> = new Set();

function getDirectParentUrn(entity: DataProduct): string | undefined {
    return entity.properties?.parentDataProduct?.urn ?? entity.parentDataProducts?.[0]?.urn;
}

function childCount(entity: DataProduct): number {
    return (entity as DataProduct & { childDataProducts?: { total?: number } }).childDataProducts?.total ?? 0;
}

/**
 * Pure builder for flat, depth-first data-product tree options (glossary-style).
 */
export function buildDataProductTreeOptions({
    entities,
    excludeSet,
    loadingSet,
    getDisplayName,
}: BuildArgs): DataProductTreeOption[] {
    const filtered = entities.filter((e) => e.type === EntityType.DataProduct && !excludeSet.has(e.urn));
    const byUrn = new Map(filtered.map((e) => [e.urn, e]));
    const childMap = new Map<string, DataProduct[]>();
    const roots: DataProduct[] = [];

    filtered.forEach((entity) => {
        const parentUrn = getDirectParentUrn(entity);
        if (!parentUrn || !byUrn.has(parentUrn)) {
            roots.push(entity);
            return;
        }
        const siblings = childMap.get(parentUrn) ?? [];
        siblings.push(entity);
        childMap.set(parentUrn, siblings);
    });

    const result: DataProductTreeOption[] = [];

    const emitLoadingPlaceholder = (parentUrn: string, depth: number, ancestorUrns: string[]) => {
        if (!loadingSet.has(parentUrn) || childMap.has(parentUrn)) return;
        result.push({
            value: `${parentUrn}::loading`,
            label: 'Loading',
            isLoadingPlaceholder: true,
            depth: depth + 1,
            ancestorUrns: [...ancestorUrns, parentUrn],
        });
    };

    const emitEntity = (entity: DataProduct, depth: number, ancestorUrns: string[]) => {
        const knownEmpty = childCount(entity) === 0;
        const hasFetchedChildren = childMap.has(entity.urn);
        result.push({
            value: entity.urn,
            label: getDisplayName(entity),
            entity,
            depth,
            ancestorUrns,
            isEmptyNode: !hasFetchedChildren && !knownEmpty,
        });

        emitLoadingPlaceholder(entity.urn, depth, ancestorUrns);

        (childMap.get(entity.urn) ?? []).forEach((child) => {
            emitEntity(child, depth + 1, [...ancestorUrns, entity.urn]);
        });
    };

    roots.forEach((root) => emitEntity(root, 0, []));
    return result;
}

type HookArgs = {
    entities: DataProduct[];
    excludeUrns?: string[];
    expandedNodes?: Set<string>;
    loadingNodeUrns?: Set<string>;
};

/**
 * Memoized tree options for the parent data-product SimpleSelect (mirrors useTermTreeOptions).
 */
export function useDataProductTreeOptions({ entities, excludeUrns, expandedNodes, loadingNodeUrns }: HookArgs) {
    const entityRegistry = useEntityRegistry();
    const excludeSet = useMemo(() => new Set(excludeUrns || []), [excludeUrns]);
    const loadingSet = loadingNodeUrns ?? EMPTY_URN_SET;

    const allOptions = useMemo(
        () =>
            buildDataProductTreeOptions({
                entities,
                excludeSet,
                loadingSet,
                getDisplayName: (entity) => entityRegistry.getDisplayName(entity.type, entity),
            }),
        [entities, excludeSet, loadingSet, entityRegistry],
    );

    const nodesWithChildren = useMemo(() => {
        const withChildren = new Set<string>();
        allOptions.forEach((opt) => {
            (opt.ancestorUrns || []).forEach((urn) => withChildren.add(urn));
        });
        allOptions.forEach((opt) => {
            if (opt.entity && childCount(opt.entity) > 0) {
                withChildren.add(opt.value);
            }
        });
        return withChildren;
    }, [allOptions]);

    const visibleOptions = useMemo(() => {
        if (!expandedNodes) return allOptions;
        return allOptions.filter((opt) => {
            const ancestors = opt.ancestorUrns || [];
            return ancestors.every((urn) => expandedNodes.has(urn));
        });
    }, [allOptions, expandedNodes]);

    return {
        allOptions,
        visibleOptions,
        nodesWithChildren,
    };
}
