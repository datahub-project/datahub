import type { Icon } from '@phosphor-icons/react';
import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { GlossaryEntityColorInput, resolveGlossaryEntityColor } from '@app/glossaryV2/colorUtils';
import { ENTITY_INDEX_FILTER_NAME } from '@app/search/utils/constants';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { ScrollAcrossEntitiesQueryVariables } from '@graphql/search.generated';
import { DisplayProperties, EntityType, GlossaryNode, GlossaryTerm, ParentNodesResult, SortOrder } from '@types';

/** Structural type for the bits of the entity registry the helpers in this file use. Keeps
 * `getCollapsedGlossaryItems` testable without needing to instantiate V1 or V2 registry. */
export type GlossaryEntityRegistryLike = {
    getDisplayName: (type: EntityType, entity: unknown) => string;
};

/** Phosphor icon component type — re-exported from `@phosphor-icons/react` so call sites import
 * it through `glossaryV2/utils` and don't have to chase the underlying package path. */
export type GlossaryIconComponent = Icon;

export const ROOT_NODES = 'rootNodes';
export const ROOT_TERMS = 'rootTerms';

export function getGlossaryRootToUpdate(entityType: EntityType) {
    return entityType === EntityType.GlossaryTerm ? ROOT_TERMS : ROOT_NODES;
}

// Get the urns or special constants for root nodes or terms (above) that need to be refreshed in the Glossary
// sidebar when making updates (edit name, create term/term group, delete term/term group, move entity)
export function getParentNodeToUpdate(entityData: GenericEntityProperties | null, entityType: EntityType) {
    return entityData?.parentNodes?.nodes?.length
        ? entityData?.parentNodes?.nodes[0]?.urn
        : getGlossaryRootToUpdate(entityType);
}

// Add the parent nodes that need to refetch from the glossary sidebar to `urnsToUpdate` state.
// This could also include ROOT_NODES or ROOT_TERMS if the item(s) that need updating don't have parents.
export function updateGlossarySidebar(
    parentNodesToUpdate: string[],
    urnsToUpdate: string[],
    setUrnsToUpdate: (updatdUrns: string[]) => void,
) {
    setUrnsToUpdate([...urnsToUpdate, ...parentNodesToUpdate]);
}

/**
 * Derive a human-readable label from a glossary URN as a last-resort fallback when no entity is
 * available to render. Glossary URNs encode the hierarchical name after the type prefix using
 * dots, e.g. `urn:li:glossaryTerm:Adoption.HighRisk` → "HighRisk". The leaf segment is what
 * users recognize; the ancestor chain (`Adoption.`) is encoded by the entity's separate
 * `parentNodes` chain when it's hydrated.
 *
 * Used by `AddTermsModal`'s chip strip when a selected URN's entity isn't in the merged cache
 * (e.g. `defaultValues` from `AdvancedFilterSelectValueModal` whose facet aggregation didn't
 * include the entity). Without this, the chip would render the raw URN like `urn:li:...`.
 */
export function deriveGlossaryLabelFromUrn(urn: string): string {
    const lastColon = urn.lastIndexOf(':');
    const id = lastColon >= 0 ? urn.slice(lastColon + 1) : urn;
    const lastDot = id.lastIndexOf('.');
    return lastDot >= 0 ? id.slice(lastDot + 1) : id;
}

/** Default page size for a single fetch of a glossary node's direct children. Real glossaries
 * with more than 50 children per node are rare; consumers that need pagination can scroll. */
export const DEFAULT_GLOSSARY_CHILDREN_COUNT = 50;

/**
 * Builds the `scrollAcrossEntities` query variables for fetching the direct children (nodes +
 * terms) of a glossary node. Shared between the glossary sidebar's `useGlossaryChildren` and the
 * tag/term picker's `useGlossaryTreeEntities` so both views agree on filter/sort.
 *
 * @param parentNodeUrn URN of the parent node whose children to load; pass `''` (or omit) for
 *     root-level (only used by code paths that need a uniform signature — root entities have
 *     dedicated queries).
 * @param scrollId      `nextScrollId` from a prior fetch when paginating, or `null` for the
 *     first page.
 * @param count         Page size (defaults to {@link DEFAULT_GLOSSARY_CHILDREN_COUNT}).
 */
/**
 * The deterministic Phosphor icon that represents a glossary entity by type — `BookmarksSimple`
 * (the "two-bookmarks" glyph) for a glossary node/group, `BookmarkSimple` (single bookmark) for a
 * term. Routes every consumer (sidebar, list cards, headers, autocomplete, parent picker) through
 * one place so the visual story stays in sync if the icon set ever changes.
 */
export function getGlossaryEntityIcon(entityType: EntityType): GlossaryIconComponent {
    return entityType === EntityType.GlossaryNode ? BookmarksSimple : BookmarkSimple;
}

/**
 * Empirical delay between a glossary create/update mutation and the moment the search index
 * starts returning the new/updated entity. `getRootGlossaryNodes`, `getRootGlossaryTerms`, and
 * `scrollAcrossEntities` (used by `useGlossaryChildren`) all read through the search index, so a
 * refetch issued immediately after the mutation typically misses the entity by several seconds.
 *
 * Used by `CreateGlossaryEntityModal` (V1 + V2) to delay the post-create refetch + analytics +
 * success-toast sequence so the new entity is more likely to be in the response. Combined with
 * the optimistic-entry mechanism in `GlossaryEntityContext` (`nodeToNewEntity`), this means the
 * user sees the new node immediately and the canonical server-side fields slot in once the
 * refetch resolves.
 *
 * If this value ever becomes unreliable (the search index slows down further), the right fix is
 * to switch the consumers to polling for the new URN to actually appear in the refetched list
 * rather than waiting a fixed interval.
 */
export const GLOSSARY_SEARCH_INDEX_REFRESH_MS = 2000;

/**
 * Shape of the optimistic entries we stash in `GlossaryEntityContext.nodeToNewEntity` after
 * creating a glossary node or term. Consumers (`useGlossaryChildren`, `GlossaryBrowser`) read
 * these like canonical glossary entities to render the new row before the search index catches
 * up — see {@link GLOSSARY_SEARCH_INDEX_REFRESH_MS} for the lag we're papering over.
 */
export type OptimisticGlossaryEntity = Pick<
    GlossaryTerm,
    'urn' | 'type' | 'properties' | 'displayProperties' | 'parentNodes'
>;

interface BuildOptimisticGlossaryEntityArgs {
    urn: string;
    entityType: EntityType;
    name: string;
    description?: string | null;
    /** The chosen color, if the user explicitly picked one. When `undefined`, the optimistic
     * entry omits `displayProperties` so the sidebar's color resolver falls through to the
     * parent / palette path — same as the canonical server-side entry will once the refetch
     * resolves. */
    colorHex?: string;
    /** The parent the entity was created under (resolved through `useEntityData()` in the
     * modal). When provided, we synthesize a `parentNodes` chain on the optimistic entry so the
     * color resolver inherits from the same root the canonical server-side entry will. */
    parent?: Pick<GlossaryNode, 'urn' | 'type' | 'displayProperties' | 'parentNodes'> | null;
}

/**
 * Build the optimistic glossary entity we stash under `nodeToNewEntity[<parent or root>]` after a
 * successful create mutation. Mirrors the eventual server shape so `resolveGlossaryEntityColor`
 * yields the same color whether the sidebar is reading the optimistic entry or the canonical
 * one — fixing a flash where a freshly-created child without an explicit color rendered with a
 * palette slot derived from its own URN instead of inheriting its parent's color.
 */
export function buildOptimisticGlossaryEntity({
    urn,
    entityType,
    name,
    description,
    colorHex,
    parent,
}: BuildOptimisticGlossaryEntityArgs): OptimisticGlossaryEntity {
    // Synthesize a direct-parent → root chain when we know the parent. GraphQL returns
    // parentNodes ordered direct-parent → root, so the parent comes first, followed by its own
    // chain (already in that order).
    let parentNodes: ParentNodesResult | undefined;
    if (parent) {
        const ancestors = parent.parentNodes?.nodes ?? [];
        const synthesized: GlossaryNode[] = [parent as GlossaryNode, ...(ancestors as GlossaryNode[])];
        parentNodes = { count: synthesized.length, nodes: synthesized };
    }
    const displayProperties: Pick<DisplayProperties, 'colorHex'> | null = colorHex ? { colorHex } : null;
    return {
        urn,
        type: entityType,
        properties: {
            name,
            description: description ?? null,
        },
        displayProperties: displayProperties as DisplayProperties | null,
        parentNodes: parentNodes ?? null,
    } as OptimisticGlossaryEntity;
}

/**
 * Build the flat list of icon-and-color items rendered in the collapsed glossary sidebar
 * (`<CollapsedItemLink>` row per root node + root term). Splitting this out of `GlossarySidebar`
 * lets us unit-test the sort + color-resolution chain without rendering React.
 */
export interface CollapsedGlossaryItem {
    urn: string;
    type: EntityType;
    name: string;
    color: string;
    Icon: GlossaryIconComponent;
}

/** Minimal shape needed to render a collapsed-sidebar row. Accepts both the canonical
 * `GlossaryNode`/`GlossaryTerm` types and the narrower GraphQL fragments
 * (`getRootGlossaryNodes`/`getRootGlossaryTerms` ↦ a subset of fields) so the helper can be
 * called with either source. */
export type CollapsedGlossaryEntityInput = GlossaryEntityColorInput & { type: EntityType };

export function getCollapsedGlossaryItems({
    nodes,
    terms,
    entityRegistry,
    generateColor,
}: {
    nodes: CollapsedGlossaryEntityInput[];
    terms: CollapsedGlossaryEntityInput[];
    entityRegistry: GlossaryEntityRegistryLike;
    generateColor: (urn: string) => string;
}): CollapsedGlossaryItem[] {
    const mapEntity = (entity: CollapsedGlossaryEntityInput): CollapsedGlossaryItem => ({
        urn: entity.urn,
        type: entity.type,
        name: entityRegistry.getDisplayName(entity.type, entity),
        // Root-level entities have no parent, so inheriting from a parent isn't possible here —
        // the resolver falls back to a palette color seeded by the entity's own URN.
        color: resolveGlossaryEntityColor(entity, generateColor),
        Icon: getGlossaryEntityIcon(entity.type),
    });
    // Sort each section by display name (locale-aware), matching how the expanded tree orders
    // siblings. We sort each section separately and concatenate nodes-then-terms so the icon
    // column stays grouped by entity type.
    const byDisplayName = (a: CollapsedGlossaryEntityInput, b: CollapsedGlossaryEntityInput) =>
        entityRegistry.getDisplayName(a.type, a).localeCompare(entityRegistry.getDisplayName(b.type, b));
    const sortedNodes = [...nodes].sort(byDisplayName);
    const sortedTerms = [...terms].sort(byDisplayName);
    return [...sortedNodes.map(mapEntity), ...sortedTerms.map(mapEntity)];
}

export function getGlossaryChildrenScrollInput(
    parentNodeUrn: string,
    scrollId: string | null = null,
    count: number = DEFAULT_GLOSSARY_CHILDREN_COUNT,
): ScrollAcrossEntitiesQueryVariables {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
            orFilters: [{ and: [{ field: 'parentNode', values: [parentNodeUrn || ''] }] }],
            count,
            sortInput: {
                sortCriteria: [
                    { field: ENTITY_INDEX_FILTER_NAME, sortOrder: SortOrder.Ascending },
                    { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending },
                ],
            },
            searchFlags: { skipCache: true },
        },
    };
}
