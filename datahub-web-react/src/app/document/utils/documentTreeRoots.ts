export type LocalRootPlacement = 'start' | 'end';

/**
 * Replace the root list with one server page while keeping roots the user
 * added locally (optimistic create, move-to-root, reveal stub).
 *
 * Previously paged server roots are dropped. Keeping every URN that was not on
 * the new first page left the previous sort's later pages stuck at the bottom.
 */
export function mergeInitializedRootUrns(
    previousRootUrns: string[],
    serverRootUrns: string[],
    localRoots: ReadonlyMap<string, LocalRootPlacement>,
): string[] {
    const serverUrns = new Set(serverRootUrns);
    const localOnly = previousRootUrns.filter((urn) => localRoots.has(urn) && !serverUrns.has(urn));
    const leading = localOnly.filter((urn) => localRoots.get(urn) !== 'end');
    const trailing = localOnly.filter((urn) => localRoots.get(urn) === 'end');
    return [...leading, ...serverRootUrns, ...trailing];
}

/**
 * Whether an async root-page response may write into tree state.
 * Page-0 loads bump `currentGeneration`; later pages reuse the active generation.
 */
export function isCurrentRootLoadGeneration(
    isMounted: boolean,
    currentGeneration: number,
    requestGeneration: number,
): boolean {
    return isMounted && requestGeneration === currentGeneration;
}

/** Combine sort + View into one infinite-scroll reset key so stale pages cannot land. */
export function documentTreeRootResetTrigger(sort: string | number, viewUrn?: string | null): string {
    return `${sort}:${viewUrn ?? ''}`;
}
