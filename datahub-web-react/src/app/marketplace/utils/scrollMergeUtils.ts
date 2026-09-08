/** Merge one scrollAcrossEntities page into accumulated results (browse + flat search). */
export function mergeScrollPageResults<T extends { urn: string }>({
    current,
    fresh,
    scrollId,
}: {
    current: T[];
    fresh: T[];
    scrollId: string | null;
}): T[] {
    if (scrollId === null) {
        return fresh;
    }

    const freshByUrn = new Map(fresh.map((entity) => [entity.urn, entity]));
    const updated = current.map((entity) => freshByUrn.get(entity.urn) || entity);
    const seenUrns = new Set(updated.map((entity) => entity.urn));
    const additions = fresh.filter((entity) => !seenUrns.has(entity.urn));

    if (additions.length === 0 && updated.every((entity, index) => entity === current[index])) {
        return current;
    }

    return [...updated, ...additions];
}
