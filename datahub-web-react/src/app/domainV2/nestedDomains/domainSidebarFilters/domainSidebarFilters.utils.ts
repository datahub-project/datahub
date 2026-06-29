import { CorpGroup, CorpUser, Domain, EntityType } from '@types';

/**
 * Structural shape these helpers operate on. Anything with an `ownership`
 * aspect — `Domain`, `ListDomainFragment`, fetched-via-search Domain, etc. —
 * is accepted, so call sites don't need to cast their hook return types up to
 * the full `Domain` shape.
 */
export type OwnedDomainLike = Pick<Domain, 'ownership'>;

/**
 * Minimal projection of an owner (CorpUser or CorpGroup) that the domain
 * sidebar's Owner filter needs:
 *   - URN (filter identity)
 *   - display name (filter label)
 *   - entity type (drives avatar shape — user vs. group)
 *   - picture (avatar image for users; groups generally have no picture)
 *
 * We extract this from the GraphQL `Ownership` aspect once at registration
 * time so the filter UI doesn't have to know about the full owner shape.
 */
export interface DomainOwnerInfo {
    urn: string;
    displayName: string;
    type: EntityType;
    pictureLink?: string | null;
}

/**
 * Best-effort display name for a CorpUser. Prefers explicit display name
 * fields (editableProperties → properties → info), falls back to full name,
 * then to the username, and finally to the raw URN.
 */
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
    return group.properties?.displayName || group.name || group.urn;
}

/**
 * Project a domain's `ownership.owners[]` into the flat owner-info shape used
 * by the sidebar filter. Skips owners with no `owner` payload (defensive — the
 * GraphQL schema marks it required, but we don't want a malformed row to crash
 * the sidebar).
 */
export function extractDomainOwners(domain: OwnedDomainLike): DomainOwnerInfo[] {
    const owners = domain.ownership?.owners ?? [];
    const out: DomainOwnerInfo[] = [];
    owners.forEach((ownerRef) => {
        const owner = ownerRef?.owner;
        if (!owner) return;

        if (owner.type === EntityType.CorpUser) {
            const user = owner as CorpUser;
            out.push({
                urn: user.urn,
                displayName: resolveUserDisplayName(user),
                type: EntityType.CorpUser,
                pictureLink: user.editableProperties?.pictureLink ?? null,
            });
        } else if (owner.type === EntityType.CorpGroup) {
            const group = owner as CorpGroup;
            out.push({
                urn: group.urn,
                displayName: resolveGroupDisplayName(group),
                type: EntityType.CorpGroup,
                pictureLink: null,
            });
        }
    });
    return out;
}

/**
 * Batch helper — flatten owners across a list of domains. Does NOT dedupe
 * (the filter context dedupes by URN on registration).
 */
export function extractOwnersFromDomains(domains: ReadonlyArray<OwnedDomainLike>): DomainOwnerInfo[] {
    return domains.flatMap(extractDomainOwners);
}

/**
 * Filter predicate: does `domain` match the sidebar's active owner selection?
 *
 *   - An empty / null `selectedOwnerUrns` means "no filter active" — every
 *     domain passes.
 *   - A non-empty selection means the domain must have at least one owner
 *     whose URN is in the selection (OR semantics, matching how the
 *     documents sidebar's author filter works).
 */
export function matchesDomainOwnerFilter(domain: OwnedDomainLike, selectedOwnerUrns: string[] | null): boolean {
    if (!selectedOwnerUrns || selectedOwnerUrns.length === 0) return true;
    const owners = domain.ownership?.owners ?? [];
    return owners.some((o) => {
        const urn = o?.owner?.urn;
        return !!urn && selectedOwnerUrns.includes(urn);
    });
}

/**
 * Batch helper — filter a list of domains by the active owner selection.
 * Returns the same element type it was given (generic preserved), so a
 * `ListDomainFragment[]` in returns `ListDomainFragment[]` out.
 */
export function filterDomainsByOwner<T extends OwnedDomainLike>(
    domains: ReadonlyArray<T>,
    selectedOwnerUrns: string[] | null,
): T[] {
    if (!selectedOwnerUrns || selectedOwnerUrns.length === 0) return [...domains];
    return domains.filter((d) => matchesDomainOwnerFilter(d, selectedOwnerUrns));
}
