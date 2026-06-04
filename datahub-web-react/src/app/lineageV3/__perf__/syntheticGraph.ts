import { Edge, Node } from 'reactflow';

/**
 * Lightweight synthetic-graph generators for the lineage rendering perf survey.
 *
 * These deliberately produce plain ReactFlow Node / Edge shapes (not full
 * LineageEntity / FetchedEntityV2 stubs) so the harness measures the cost of
 * mounting the React Flow tree + a representative node component, isolated
 * from upstream concerns like Apollo, the entity registry, or compute-graph
 * layout. Real-world numbers will be larger than what we report here — the
 * goal is to surface scaling behaviour and the relative effect of toggles
 * like `onlyRenderVisibleElements`, not to predict production frame time.
 */

export interface SyntheticGraph {
    nodes: Node<{ urn: string; label: string }>[];
    edges: Edge[];
    /** Roughly the bounding box the layout occupies — useful for tuning viewport tests. */
    bounds: { width: number; height: number };
}

const NODE_W = 320;
const NODE_H = 96;
const GAP_X = 80;
const GAP_Y = 32;

const STUB_NODE_TYPE = 'stub-entity';

/**
 * `flat`: N nodes arranged in a single column. Edges chain consecutive nodes
 * so we always have at least one edge per node (closer to lineage shapes than
 * an empty edge set). No nesting.
 */
export function makeFlatGraph(n: number): SyntheticGraph {
    const nodes: SyntheticGraph['nodes'] = [];
    const edges: SyntheticGraph['edges'] = [];
    for (let i = 0; i < n; i += 1) {
        const urn = `urn:li:dataset:(urn:li:dataPlatform:perf,table_${i},PROD)`;
        nodes.push({
            id: urn,
            type: STUB_NODE_TYPE,
            position: { x: 0, y: i * (NODE_H + GAP_Y) },
            data: { urn, label: `table_${i}` },
            width: NODE_W,
            height: NODE_H,
        });
        if (i > 0) {
            const prevUrn = `urn:li:dataset:(urn:li:dataPlatform:perf,table_${i - 1},PROD)`;
            edges.push({ id: `e_${i - 1}_${i}`, source: prevUrn, target: urn });
        }
    }
    return {
        nodes,
        edges,
        bounds: { width: NODE_W, height: n * (NODE_H + GAP_Y) },
    };
}

/**
 * `nested`: Domain bbox → P member DP bboxes → A assets per DP. Approximates
 * the Domain lineage view's worst-case shape. Each asset has an upstream edge
 * to the previous asset in the same DP, and each DP has a cross-DP edge to
 * the next DP so we exercise inter-bbox edges too.
 */
export function makeNestedGraph(domainCount: number, dpsPerDomain: number, assetsPerDp: number): SyntheticGraph {
    const nodes: SyntheticGraph['nodes'] = [];
    const edges: SyntheticGraph['edges'] = [];

    let cursorY = 0;
    const dpBoxW = NODE_W + 80;
    const dpBoxAssetGap = NODE_H + 16;

    for (let d = 0; d < domainCount; d += 1) {
        const domainUrn = `urn:li:domain:perf_${d}`;
        const dpsHeight = dpsPerDomain * (assetsPerDp * dpBoxAssetGap + 120);
        nodes.push({
            id: domainUrn,
            type: STUB_NODE_TYPE,
            position: { x: 0, y: cursorY },
            data: { urn: domainUrn, label: `Domain ${d}` },
            width: dpBoxW + 64,
            height: dpsHeight + 80,
            style: { width: dpBoxW + 64, height: dpsHeight + 80 },
        });
        let dpY = 40;
        let prevDpUrn: string | null = null;
        for (let p = 0; p < dpsPerDomain; p += 1) {
            const dpUrn = `urn:li:dataProduct:perf_${d}_${p}`;
            const dpBoxHeight = assetsPerDp * dpBoxAssetGap + 56;
            nodes.push({
                id: dpUrn,
                type: STUB_NODE_TYPE,
                position: { x: 32, y: dpY },
                data: { urn: dpUrn, label: `DP ${d}.${p}` },
                width: dpBoxW,
                height: dpBoxHeight,
                style: { width: dpBoxW, height: dpBoxHeight },
                parentNode: domainUrn,
                extent: 'parent',
            });
            if (prevDpUrn) {
                edges.push({ id: `dp_${prevDpUrn}_${dpUrn}`, source: prevDpUrn, target: dpUrn });
            }
            let prevAssetUrn: string | null = null;
            for (let a = 0; a < assetsPerDp; a += 1) {
                const assetUrn = `urn:li:dataset:(urn:li:dataPlatform:perf,d${d}_p${p}_t${a},PROD)`;
                nodes.push({
                    id: assetUrn,
                    type: STUB_NODE_TYPE,
                    position: { x: 16, y: 32 + a * dpBoxAssetGap },
                    data: { urn: assetUrn, label: `t${a}` },
                    width: NODE_W,
                    height: NODE_H,
                    parentNode: dpUrn,
                    extent: 'parent',
                });
                if (prevAssetUrn) {
                    edges.push({ id: `a_${prevAssetUrn}_${assetUrn}`, source: prevAssetUrn, target: assetUrn });
                }
                prevAssetUrn = assetUrn;
            }
            prevDpUrn = dpUrn;
            dpY += dpBoxHeight + 24;
        }
        cursorY += dpsHeight + 120;
    }
    return {
        nodes,
        edges,
        bounds: { width: dpBoxW + 64, height: cursorY },
    };
}

/**
 * `dense`: N nodes laid out in a grid, with every node connected to up to
 * `edgesPerNode` arbitrary other nodes. Exercises the edge-rendering path
 * specifically — SVG paths are usually the second-largest render cost after
 * node DOM, so this surfaces edge scaling separately from node scaling.
 */
export function makeDenseGraph(n: number, edgesPerNode: number): SyntheticGraph {
    const nodes: SyntheticGraph['nodes'] = [];
    const edges: SyntheticGraph['edges'] = [];
    const cols = Math.max(1, Math.ceil(Math.sqrt(n)));
    for (let i = 0; i < n; i += 1) {
        const urn = `urn:li:dataset:(urn:li:dataPlatform:perf,dense_${i},PROD)`;
        const row = Math.floor(i / cols);
        const col = i % cols;
        nodes.push({
            id: urn,
            type: STUB_NODE_TYPE,
            position: { x: col * (NODE_W + GAP_X), y: row * (NODE_H + GAP_Y) },
            data: { urn, label: `dense_${i}` },
            width: NODE_W,
            height: NODE_H,
        });
    }
    for (let i = 0; i < n; i += 1) {
        for (let k = 1; k <= edgesPerNode; k += 1) {
            const targetIdx = (i + k) % n;
            if (targetIdx !== i) {
                const sourceUrn = `urn:li:dataset:(urn:li:dataPlatform:perf,dense_${i},PROD)`;
                const targetUrn = `urn:li:dataset:(urn:li:dataPlatform:perf,dense_${targetIdx},PROD)`;
                edges.push({ id: `e_${i}_${targetIdx}`, source: sourceUrn, target: targetUrn });
            }
        }
    }
    return {
        nodes,
        edges,
        bounds: { width: cols * (NODE_W + GAP_X), height: Math.ceil(n / cols) * (NODE_H + GAP_Y) },
    };
}

export const PERF_STUB_NODE_TYPE = STUB_NODE_TYPE;
