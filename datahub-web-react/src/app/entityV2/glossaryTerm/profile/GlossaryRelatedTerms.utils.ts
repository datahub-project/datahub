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
    fetchedSettled: boolean,
): ReadonlyArray<unknown> {
    // While getRelatedTerms is in flight, including the refetch after an edit, show the profile
    // list. A settled fetch is the list for this tab, so a longer cached fetch cannot hide a term
    // that was just removed. An empty settled fetch still falls back to a non-empty profile list.
    if (!fetchedSettled) {
        return profileRelationships;
    }
    if (fetchedRelationships.length > 0) {
        return fetchedRelationships;
    }
    return profileRelationships.length > 0 ? profileRelationships : fetchedRelationships;
}

export function glossaryRelatedTermsArePending({
    urn,
    relationshipCount,
    profileReady,
    fetchedSettled,
}: {
    urn?: string;
    relationshipCount: number;
    profileReady: boolean;
    fetchedSettled: boolean;
}): boolean {
    if (!urn || relationshipCount > 0) {
        return false;
    }
    if (!fetchedSettled) {
        return true;
    }
    return !profileReady;
}
