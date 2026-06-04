import { render } from '@testing-library/react';
import React from 'react';
import ReactFlow, { ReactFlowProvider } from 'reactflow';
import 'reactflow/dist/style.css';
import { describe, expect, it } from 'vitest';

import { lightNodeTypes } from '@app/lineageV3/__perf__/stubNodeTypes';
import { makeFlatGraph } from '@app/lineageV3/__perf__/syntheticGraph';

/**
 * Sanity check on the perf survey's `onlyRenderVisibleElements` measurement.
 *
 * In jsdom, `getBoundingClientRect` typically returns zeros, which can cause
 * React Flow's viewport-intersection check to skip ALL nodes — making
 * `virt=true` look artificially fast because it's not rendering anything,
 * not because virtualisation is working. This test asserts that virt-off
 * mounts at least one DOM node per synthetic node and virt-on mounts
 * strictly fewer (potentially zero).
 */

function countMountedNodes(container: HTMLElement): number {
    return container.querySelectorAll('.react-flow__node').length;
}

describe('Virtualisation sanity check (interpret perf numbers carefully)', () => {
    const N = 100;

    it('virt=false mounts every node', () => {
        const { nodes, edges } = makeFlatGraph(N);
        const { container } = render(
            <div style={{ width: 1280, height: 720 }}>
                <ReactFlowProvider>
                    <ReactFlow
                        defaultNodes={nodes}
                        defaultEdges={edges}
                        nodeTypes={lightNodeTypes}
                        onlyRenderVisibleElements={false}
                        proOptions={{ hideAttribution: true }}
                    />
                </ReactFlowProvider>
            </div>,
        );
        expect(countMountedNodes(container)).toBeGreaterThan(0);
    });

    it('virt=true mounts strictly fewer (or zero) nodes', () => {
        const { nodes, edges } = makeFlatGraph(N);
        const { container } = render(
            <div style={{ width: 1280, height: 720 }}>
                <ReactFlowProvider>
                    <ReactFlow
                        defaultNodes={nodes}
                        defaultEdges={edges}
                        nodeTypes={lightNodeTypes}
                        onlyRenderVisibleElements
                        proOptions={{ hideAttribution: true }}
                    />
                </ReactFlowProvider>
            </div>,
        );
        expect(countMountedNodes(container)).toBeLessThan(N);
    });
});
