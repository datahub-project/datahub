type WithRemovedStatus = { status?: { removed?: boolean | null } | null } | null | undefined;

/**
 * Soft-deleting a tag or glossary term sets Status.removed on it, but the associations already stored on
 * assets stay put and the read path returns them. Every surface that lists or counts an asset's labels
 * must skip these stale edges.
 */
export function isRemovedAssociation(association: { tag?: WithRemovedStatus; term?: WithRemovedStatus }): boolean {
    return !!(association.tag ?? association.term)?.status?.removed;
}
