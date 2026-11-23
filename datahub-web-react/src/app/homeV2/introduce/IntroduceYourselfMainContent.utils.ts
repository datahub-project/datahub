type PlatformListEntry = {
    platform: { urn: string };
};

/**
 * Keep already-selected fallback platforms in the list when a later source
 * (for example recommendations) replaces the options the user picked from.
 */
export function platformsWithRetainedSelections<T extends PlatformListEntry>(
    basePlatforms: T[],
    fallbackPlatforms: T[],
    selectedUrns: string[],
): T[] {
    const visibleUrns = new Set(basePlatforms.map((entry) => entry.platform.urn));
    const retained = fallbackPlatforms.filter(
        (entry) => selectedUrns.includes(entry.platform.urn) && !visibleUrns.has(entry.platform.urn),
    );
    return retained.length > 0 ? [...basePlatforms, ...retained] : basePlatforms;
}
