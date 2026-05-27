/**
 * Overscan-aware DOM virtualisation for lineage graphs.
 *
 * React Flow's `onlyRenderVisibleElements` filters against the exact viewport,
 * so nodes pop in at the trailing edge during a pan. This hook reproduces the
 * filter but inflates the viewport by {@link DEFAULT_OVERSCAN_FACTOR} on each
 * side, eliminating pop-in within one buffer's worth of drag at the cost of
 * mounting more DOM.
 *
 * Must be called inside the `<ReactFlow>` subtree (uses `useStore`). The hook
 * never re-renders parents on viewport changes — it writes `hidden` flags
 * directly via `setNodes`, and preserves object identity for unchanged nodes
 * so `React.memo` on `LineageEntityNode` still skips re-renders.
 */
import { useEffect, useRef } from 'react';
import { useReactFlow, useStore } from 'reactflow';

import type { LineageVisualizationNode } from '@app/lineageV2/NodeBuilder';
import { DEFAULT_OVERSCAN_FACTOR } from '@app/lineageV2/perfFlags';

// Conservative fallbacks for nodes React Flow hasn't measured yet — wider /
// taller than real nodes so a late-arriving measurement can't accidentally
// exclude a node from the overscan rect.
const FALLBACK_NODE_WIDTH = 320;
const FALLBACK_NODE_HEIGHT = 200;

export function useOverscanVirt(enabled: boolean, overscanFactor: number = DEFAULT_OVERSCAN_FACTOR): void {
    const reactFlow = useReactFlow();
    const transform = useStore((s) => s.transform);
    const width = useStore((s) => s.width);
    const height = useStore((s) => s.height);
    // Short-circuit consecutive viewport events that resolve to the same rect
    // (d3 emits sub-pixel transform updates during inertia scroll).
    const lastRectKeyRef = useRef<string>('');
    const everEnabledRef = useRef<boolean>(false);

    useEffect(() => {
        if (!enabled) {
            if (everEnabledRef.current) {
                reactFlow.setNodes((nodes) => nodes.map((n) => (n.hidden ? { ...n, hidden: false } : n)));
                everEnabledRef.current = false;
                lastRectKeyRef.current = '';
            }
            return;
        }
        everEnabledRef.current = true;

        const [tx, ty, zoom] = transform;
        if (!zoom || zoom <= 0 || !width || !height) return;

        // Screen → flow coords; `node.position` lives in flow coords.
        const flowViewportWidth = width / zoom;
        const flowViewportHeight = height / zoom;
        const flowViewportX = -tx / zoom;
        const flowViewportY = -ty / zoom;

        const overscanX = flowViewportX - flowViewportWidth * overscanFactor;
        const overscanY = flowViewportY - flowViewportHeight * overscanFactor;
        const overscanWidth = flowViewportWidth * (1 + 2 * overscanFactor);
        const overscanHeight = flowViewportHeight * (1 + 2 * overscanFactor);

        const rectKey =
            `${Math.floor(overscanX)}|${Math.floor(overscanY)}|` +
            `${Math.floor(overscanWidth)}|${Math.floor(overscanHeight)}`;
        if (rectKey === lastRectKeyRef.current) return;
        lastRectKeyRef.current = rectKey;

        reactFlow.setNodes((nodes) => {
            let mutated = false;
            const next = nodes.map((node) => {
                const n = node as LineageVisualizationNode & { hidden?: boolean };
                const w = n.width ?? FALLBACK_NODE_WIDTH;
                const h = n.height ?? FALLBACK_NODE_HEIGHT;
                const nx = n.position?.x ?? 0;
                const ny = n.position?.y ?? 0;
                const intersects =
                    nx < overscanX + overscanWidth &&
                    nx + w > overscanX &&
                    ny < overscanY + overscanHeight &&
                    ny + h > overscanY;
                const shouldHide = !intersects;
                if (Boolean(n.hidden) === shouldHide) return node;
                mutated = true;
                return { ...node, hidden: shouldHide };
            });
            return mutated ? next : nodes;
        });
    }, [enabled, overscanFactor, transform, width, height, reactFlow]);

    // Restore `hidden` flags on unmount so toggling overscan off mid-session
    // doesn't leave nodes permanently invisible.
    useEffect(() => {
        return () => {
            reactFlow.setNodes((nodes) => nodes.map((n) => (n.hidden ? { ...n, hidden: false } : n)));
        };
    }, [reactFlow]);
}
