type RelationshipGroup = {
    relationships?: ReadonlyArray<unknown> | null;
} | null;

// Profile `getGlossaryTerm` and `getRelatedTerms` store relationship groups under different
// argument keys. Read whichever list is populated for the selected group.
export function relationshipEntries(
    source: Record<string, RelationshipGroup> | null | undefined,
    selectedKey: string,
): ReadonlyArray<unknown> {
    const relationships = source?.[selectedKey]?.relationships;
    return Array.isArray(relationships) ? relationships : [];
}

function relationshipUrn(entry: unknown): string {
    if (!entry || typeof entry !== 'object') {
        return '';
    }
    return (entry as { entity?: { urn?: string } }).entity?.urn ?? '';
}

// Add and remove refresh the profile payload only. A changed signature means that payload is newer
// than the cached getRelatedTerms result, so the tab refetches instead of keeping the old list.
export function glossaryRelationshipSignature(
    source: Record<string, RelationshipGroup> | null | undefined,
    keys: readonly string[],
): string {
    return keys.map((key) => relationshipEntries(source, key).map(relationshipUrn).join(',')).join('|');
}

export function pickGlossaryRelatedRelationships(
    profileRelationships: ReadonlyArray<unknown>,
    fetchedRelationships: ReadonlyArray<unknown>,
    {
        fetchedSettled,
        holdForProfile,
        fetchFailed,
    }: {
        fetchedSettled: boolean;
        // True after an add or remove until getRelatedTerms returns the same urns as the profile.
        holdForProfile: boolean;
        fetchFailed: boolean;
    },
): ReadonlyArray<unknown> {
    // A settled fetch that has not caught up to the edited profile, or one that failed and left
    // the previous payload in place, must not put a removed term back on screen. An empty settled
    // fetch is itself the result: a stale non-empty profile does not replace it.
    if (!fetchedSettled || holdForProfile || fetchFailed) {
        return profileRelationships;
    }
    return fetchedRelationships;
}

export function glossaryRelatedTermsArePending({
    urn,
    relationshipCount,
    fetchedSettled,
}: {
    urn?: string;
    relationshipCount: number;
    fetchedSettled: boolean;
}): boolean {
    if (!urn || relationshipCount > 0) {
        return false;
    }
    // A term with no relationships is an empty tab once this urn's query has settled. Waiting on
    // the profile urn leaves the loader up when that refetch fails.
    return !fetchedSettled;
}
