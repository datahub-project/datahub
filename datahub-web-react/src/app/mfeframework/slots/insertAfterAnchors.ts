/**
 * Places `items` into `host`, in order. An item whose `after` names the key of an element already in the list
 * (matched case-insensitively) goes right after it, and after any item placed there before it; otherwise it
 * is appended. Used to position slot tabs among the host's tabs and slot sections among its sidebar sections.
 */
export function insertAfterAnchors<T>(
    host: T[],
    items: { item: T; after?: string }[],
    keyOf: (element: T) => string | undefined,
): T[] {
    const result = [...host];
    const lastPlacedAfter = new Map<string, T>();
    items.forEach(({ item, after }) => {
        const target = after?.trim().toLowerCase();
        const anchor = target
            ? (lastPlacedAfter.get(target) ?? result.find((element) => keyOf(element)?.toLowerCase() === target))
            : undefined;
        if (!target || anchor === undefined) {
            result.push(item);
            return;
        }
        result.splice(result.indexOf(anchor) + 1, 0, item);
        lastPlacedAfter.set(target, item);
    });
    return result;
}
