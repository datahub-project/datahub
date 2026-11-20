import { useCallback, useRef } from 'react';

/**
 * React keys for a list of operands, kept in a list parallel to the operands themselves.
 *
 * Predicates carry no id and are rebuilt as new objects on every edit, so neither the index nor object
 * identity is stable. The keys live here rather than on the predicates so nothing extra is ever saved
 * or sent to GMS. Removals go through `Operands`, which calls `markRemoval` so the right key is dropped;
 * additions are always appended at the end. Any other change in length keeps the leading keys, which is
 * the same as keying by index.
 */
export function useOperandKeys(count: number) {
    const keysRef = useRef<number[]>([]);
    const nextKeyRef = useRef(0);
    // Index passed to the last `markRemoval`; cleared whenever the operand count changes.
    const pendingRemovalRef = useRef<number | null>(null);

    let keys = keysRef.current;
    if (keys.length !== count) {
        const pending = pendingRemovalRef.current;
        if (pending !== null && count === keys.length - 1) {
            keys = keys.filter((_, i) => i !== pending);
        } else if (count > keys.length) {
            const added = Array.from({ length: count - keys.length }, () => {
                nextKeyRef.current += 1;
                return nextKeyRef.current;
            });
            keys = [...keys, ...added];
        } else {
            keys = keys.slice(0, count);
        }
        keysRef.current = keys;
        pendingRemovalRef.current = null;
    }

    const markRemoval = useCallback((index: number) => {
        pendingRemovalRef.current = index;
    }, []);

    return { keys, markRemoval };
}
