type UrnEntity = {
    urn: string;
};

/**
 * Merge one scroll page into an accumulated URN list.
 * First pages replace the list; later pages refresh existing rows and append new ones.
 */
export function mergeUrnEntities<T extends UrnEntity>(current: T[], fresh: T[], isFirstPage: boolean): T[] {
    if (isFirstPage) return fresh;

    const freshByUrn = new Map(fresh.map((entity) => [entity.urn, entity]));
    const updated = current.map((entity) => freshByUrn.get(entity.urn) ?? entity);
    const seenUrns = new Set(updated.map((entity) => entity.urn));
    const additions: T[] = [];
    fresh.forEach((entity) => {
        if (seenUrns.has(entity.urn)) return;
        seenUrns.add(entity.urn);
        additions.push(entity);
    });

    if (additions.length === 0 && updated.every((entity, index) => entity === current[index])) {
        return current;
    }

    return [...updated, ...additions];
}

/** Prepend entities that are not already present, preserving current order. */
export function prependMissingUrnEntities<T extends UrnEntity>(current: T[], extras: T[]): T[] {
    if (extras.length === 0) return current;
    const existingUrns = new Set(current.map((entity) => entity.urn));
    const additions = extras.filter((entity) => !existingUrns.has(entity.urn));
    return additions.length > 0 ? [...additions, ...current] : current;
}
