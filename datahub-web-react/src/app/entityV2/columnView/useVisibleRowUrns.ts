import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

/**
 * Tracks which table rows are on screen. Cells call `probeRef(urn)` to get a ref callback for a
 * tiny probe element; one shared IntersectionObserver reports entries and `visibleUrns` is
 * updated once per animation frame. Rows that scroll away are removed, so consumers see the
 * current viewport (they keep their own cache — see useRelationshipColumnData).
 */
export function useVisibleRowUrns(root?: Element | null) {
    const [visibleUrns, setVisibleUrns] = useState<string[]>([]);
    const visibleRef = useRef<Set<string>>(new Set());
    const elementUrn = useRef<WeakMap<Element, string>>(new WeakMap());
    const observerRef = useRef<IntersectionObserver | null>(null);
    const frameRef = useRef<number | null>(null);
    const refCallbacks = useRef<Map<string, (el: Element | null) => void>>(new Map());

    const flush = useCallback(() => {
        frameRef.current = null;
        setVisibleUrns(Array.from(visibleRef.current));
    }, []);

    useEffect(() => {
        if (typeof IntersectionObserver === 'undefined') return undefined;
        const observer = new IntersectionObserver(
            (entries) => {
                entries.forEach((entry) => {
                    const urn = elementUrn.current.get(entry.target);
                    if (!urn) return;
                    if (entry.isIntersecting) visibleRef.current.add(urn);
                    else visibleRef.current.delete(urn);
                });
                if (frameRef.current === null) frameRef.current = requestAnimationFrame(flush);
            },
            { root: root ?? null, rootMargin: '200px 0px' },
        );
        observerRef.current = observer;
        return () => {
            observer.disconnect();
            observerRef.current = null;
            if (frameRef.current !== null) cancelAnimationFrame(frameRef.current);
        };
    }, [root, flush]);

    /** Stable per-urn ref callback; attach to a probe element inside the row. */
    const probeRef = useCallback((urn: string) => {
        let cb = refCallbacks.current.get(urn);
        if (!cb) {
            cb = (el: Element | null) => {
                const observer = observerRef.current;
                if (!observer) {
                    // No IntersectionObserver (tests / old browsers): treat every mounted row as visible.
                    if (el) visibleRef.current.add(urn);
                    else visibleRef.current.delete(urn);
                    if (frameRef.current === null && typeof requestAnimationFrame !== 'undefined') {
                        frameRef.current = requestAnimationFrame(flush);
                    }
                    return;
                }
                if (el) {
                    elementUrn.current.set(el, urn);
                    observer.observe(el);
                } else {
                    visibleRef.current.delete(urn);
                }
            };
            refCallbacks.current.set(urn, cb);
        }
        return cb;
    }, [flush]);

    return useMemo(() => ({ visibleUrns, probeRef }), [visibleUrns, probeRef]);
}

export type VisibleRows = ReturnType<typeof useVisibleRowUrns>;
