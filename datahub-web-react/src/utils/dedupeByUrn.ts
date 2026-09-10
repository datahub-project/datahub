// When the same urn appears more than once, prefer the entry where isPropagated is false, since a manually
// applied entry should win over one that arrived via automated propagation. Ties keep the first occurrence.
export function dedupeByUrn<T>(items: T[], getUrn: (item: T) => string, isPropagated?: (item: T) => boolean): T[] {
    const bestByUrn = new Map<string, T>();
    const urnOrder: string[] = [];

    items.forEach((item) => {
        const urn = getUrn(item);
        const existing = bestByUrn.get(urn);
        if (!existing) {
            bestByUrn.set(urn, item);
            urnOrder.push(urn);
            return;
        }
        if (isPropagated?.(existing) && !isPropagated?.(item)) {
            bestByUrn.set(urn, item);
        }
    });

    return urnOrder.map((urn) => bestByUrn.get(urn) as T);
}
