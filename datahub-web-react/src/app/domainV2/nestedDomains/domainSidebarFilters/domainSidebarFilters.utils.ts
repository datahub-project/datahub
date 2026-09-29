import { CorpGroup, CorpUser, EntityType, FacetMetadata } from '@types';

/**
 * Minimal projection of an owner (CorpUser or CorpGroup) that the domain
 * sidebar's Owner filter needs:
 *   - URN (filter identity, sent back to the server as the filter value)
 *   - display name (filter label)
 *   - entity type (drives avatar shape — user vs. group)
 *   - picture (avatar image for users; absent here because the search-facet
 *     `entityDisplayNameFields` projection does not include
 *     `editableProperties.pictureLink`; widening that shared fragment for the
 *     sake of the filter would inflate every other search response in the
 *     app, so the Avatar gracefully falls back to initials)
 */
export interface DomainOwnerInfo {
    urn: string;
    displayName: string;
    type: EntityType;
    pictureLink?: string | null;
}

function resolveUserDisplayName(user: CorpUser): string {
    return (
        user.editableProperties?.displayName ||
        user.properties?.displayName ||
        user.info?.displayName ||
        user.properties?.fullName ||
        user.info?.fullName ||
        user.username ||
        user.urn
    );
}

function resolveGroupDisplayName(group: CorpGroup): string {
    return group.properties?.displayName || group.info?.displayName || group.name || group.urn;
}

/**
 * Pull the "owners" facet out of a `scrollAcrossEntities` response and
 * project each aggregation into the flat shape the Owner multi-select needs.
 *
 * Why server-side aggregations and not client-side collection?
 *   The sidebar lazy-loads domains 25 at a time via `useScrollDomains`. A
 *   client-side walk of loaded `DomainNode`s would only ever see the owners
 *   attached to the first page (or however far the user has scrolled), so the
 *   dropdown would silently hide owners that exist on later pages. The server
 *   already computes a deduped owner aggregation across the full filtered
 *   dataset — read that instead.
 *
 * Aggregations whose `entity` is missing or is neither a CorpUser nor a
 * CorpGroup are skipped (the schema marks `entity` as nullable; we don't want
 * a malformed row to crash the sidebar).
 */
export function extractOwnerOptionsFromFacets(
    facets: ReadonlyArray<FacetMetadata> | null | undefined,
): DomainOwnerInfo[] {
    if (!facets || facets.length === 0) return [];
    const ownersFacet = facets.find((f) => f.field === 'owners');
    if (!ownersFacet) return [];

    const out: DomainOwnerInfo[] = [];
    ownersFacet.aggregations.forEach((agg) => {
        const { entity } = agg;
        if (!entity) return;

        if (entity.type === EntityType.CorpUser) {
            const user = entity as CorpUser;
            out.push({
                urn: user.urn,
                displayName: resolveUserDisplayName(user),
                type: EntityType.CorpUser,
            });
        } else if (entity.type === EntityType.CorpGroup) {
            const group = entity as CorpGroup;
            out.push({
                urn: group.urn,
                displayName: resolveGroupDisplayName(group),
                type: EntityType.CorpGroup,
            });
        }
    });
    return out;
}
