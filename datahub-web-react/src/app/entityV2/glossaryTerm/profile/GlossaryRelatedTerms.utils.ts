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

export function pickGlossaryRelatedRelationships(
    profileRelationships: ReadonlyArray<unknown>,
    fetchedRelationships: ReadonlyArray<unknown>,
): ReadonlyArray<unknown> {
    if (fetchedRelationships.length > profileRelationships.length) {
        return fetchedRelationships;
    }
    if (profileRelationships.length > 0) {
        return profileRelationships;
    }
    return fetchedRelationships;
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
