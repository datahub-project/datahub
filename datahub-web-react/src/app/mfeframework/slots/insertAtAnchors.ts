/**
 * Places `items` into `host`, in order. An item whose `after` names the key of an element already in the list
 * (matched case-insensitively) goes right after it, and after any item placed there before it. An item whose
 * `before` names one goes right before it, and after any item placed before it earlier. Otherwise, or when
 * the named element is not in the list yet, the item is appended. Because items are placed in order, an item
 * can anchor to an earlier item but not to a later one. Used to position slot tabs among the host's tabs and
 * slot sections among its sidebar sections.
 */
export function insertAtAnchors<T>(
    host: T[],
    items: { item: T; after?: string; before?: string }[],
    keyOf: (element: T) => string | undefined,
): T[] {
    const result = [...host];
    const lastPlacedAfter = new Map<string, T>();
    const findByKey = (target: string) => result.find((element) => keyOf(element)?.toLowerCase() === target);
    items.forEach(({ item, after, before }) => {
        const afterTarget = after?.trim().toLowerCase();
        const beforeTarget = before?.trim().toLowerCase();
        if (afterTarget) {
            const anchor = lastPlacedAfter.get(afterTarget) ?? findByKey(afterTarget);
            if (anchor !== undefined) {
                result.splice(result.indexOf(anchor) + 1, 0, item);
                lastPlacedAfter.set(afterTarget, item);
                return;
            }
        } else if (beforeTarget) {
            // Inserting directly in front of the anchor keeps config order: each later item lands between the
            // earlier ones and the anchor.
            const anchor = findByKey(beforeTarget);
            if (anchor !== undefined) {
                result.splice(result.indexOf(anchor), 0, item);
                return;
            }
        }
        result.push(item);
    });
    return result;
}
