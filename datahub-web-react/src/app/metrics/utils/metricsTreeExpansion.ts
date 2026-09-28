import { mapPool } from '@app/document/utils/documentTreeExpansion';

/**
 * Caps for metrics section expand-all. Same idea as Documents — uncapped BFS
 * would freeze large semantic-model trees.
 */
export const METRICS_EXPAND_ALL_MAX_DEPTH = 3;
export const METRICS_EXPAND_ALL_MAX_NODES = 75;
export const METRICS_EXPAND_ALL_CONCURRENCY = 6;

export type MetricsExpandableNode = {
    urn: string;
    hasChildren: boolean;
};

export type MetricsExpandParent = { kind: 'model'; urn: string } | { kind: 'metric'; urn: string };

export type ExpandAllMetricsTreeResult = {
    truncated: boolean;
    nodesExpanded: number;
};

interface ExpandAllMetricsTreeArgs {
    /** Semantic-model roots currently in the sidebar tree. */
    modelRoots: MetricsExpandableNode[];
    loadChildren: (parent: MetricsExpandParent) => Promise<MetricsExpandableNode[]>;
    onExpandModels: (urns: string[]) => void;
    onExpandMetrics: (urns: string[]) => void;
    maxDepth?: number;
    maxNodes?: number;
    concurrency?: number;
}

/**
 * Breadth-first expand of semantic models then nested metrics, capped by depth,
 * node count, and fetch concurrency (Documents `expandAllFolders` pattern).
 *
 * Wave 0 expands models and loads their root metrics. Later waves expand metrics
 * that have children and load deeper metric children.
 */
export async function expandAllMetricsTree({
    modelRoots,
    loadChildren,
    onExpandModels,
    onExpandMetrics,
    maxDepth = METRICS_EXPAND_ALL_MAX_DEPTH,
    maxNodes = METRICS_EXPAND_ALL_MAX_NODES,
    concurrency = METRICS_EXPAND_ALL_CONCURRENCY,
}: ExpandAllMetricsTreeArgs): Promise<ExpandAllMetricsTreeResult> {
    const seen = new Set<string>();
    let nodesExpanded = 0;
    let truncated = false;

    const enqueue = (urns: string[]): string[] => {
        const fresh: string[] = [];
        urns.forEach((urn) => {
            if (!seen.has(urn)) {
                seen.add(urn);
                fresh.push(urn);
            }
        });
        return fresh;
    };

    const takeBatch = (urns: string[]): string[] => {
        const remaining = maxNodes - nodesExpanded;
        if (remaining <= 0) {
            truncated = true;
            return [];
        }
        if (urns.length > remaining) {
            truncated = true;
            return urns.slice(0, remaining);
        }
        return urns;
    };

    // Wave 0 — semantic models
    const modelBatch = takeBatch(enqueue(modelRoots.filter((n) => n.hasChildren).map((n) => n.urn)));
    if (modelBatch.length === 0) {
        return { truncated, nodesExpanded };
    }

    onExpandModels(modelBatch);
    nodesExpanded += modelBatch.length;

    const modelChildResults = await mapPool(modelBatch, concurrency, (urn) => loadChildren({ kind: 'model', urn }));

    let currentMetricUrns = enqueue(
        modelChildResults.flatMap((children) => children.filter((c) => c.hasChildren).map((c) => c.urn)),
    );
    let depth = 1;

    while (currentMetricUrns.length > 0) {
        if (depth >= maxDepth) {
            truncated = true;
            break;
        }

        const batch = takeBatch(currentMetricUrns);
        if (batch.length === 0) break;

        onExpandMetrics(batch);
        nodesExpanded += batch.length;

        // eslint-disable-next-line no-await-in-loop
        const results = await mapPool(batch, concurrency, (urn) => loadChildren({ kind: 'metric', urn }));
        const candidates: string[] = [];
        results.forEach((children) => {
            children.forEach((child) => {
                if (child.hasChildren) candidates.push(child.urn);
            });
        });

        depth += 1;
        currentMetricUrns = enqueue(candidates);
        if (currentMetricUrns.length > 0 && (depth >= maxDepth || nodesExpanded >= maxNodes)) {
            truncated = true;
            currentMetricUrns = [];
        }
    }

    return { truncated, nodesExpanded };
}
