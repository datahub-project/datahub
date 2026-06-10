import { GenericEntityProperties } from '@app/entity/shared/types';
import { ENTITY_INDEX_FILTER_NAME } from '@app/search/utils/constants';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { ScrollAcrossEntitiesQueryVariables } from '@graphql/search.generated';
import { EntityType, SortOrder } from '@types';

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
