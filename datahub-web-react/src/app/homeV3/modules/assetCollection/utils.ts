export function sortByUrnOrder<T extends { urn: string }>(objects: T[], orderedUrns: string[]): T[] {
    // Create a map of urn to index for O(1) lookup
    const urnToIndex = new Map<string, number>();
    orderedUrns.forEach((urn, index) => {
        if (!urnToIndex.has(urn)) {
            urnToIndex.set(urn, index);
        }
    });

    // Sort objects based on the index of their ID in the ordered list
    return [...objects].sort((a, b) => {
        const indexA = urnToIndex.get(a.urn);
        const indexB = urnToIndex.get(b.urn);

        // Handle cases where ID might not be in the ordered list
        if (indexA === undefined && indexB === undefined) return 0;
        if (indexA === undefined) return 1; // a goes to end
        if (indexB === undefined) return -1; // b goes to end

        return indexA - indexB;
    });
}
