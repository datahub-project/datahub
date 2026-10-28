import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

/**
 * Caps for section expand-all. Uncapped BFS + Promise.all freezes large libraries.
 * Single-caret expand is unaffected (one node, one page of children).
 */
export const EXPAND_ALL_MAX_DEPTH = 3;
export const EXPAND_ALL_MAX_FOLDERS = 75;
export const EXPAND_ALL_CONCURRENCY = 6;

/**
 * Collect the urns of every expandable node (a node that `hasChildren`) reachable
 * from `roots` through the already-loaded tree, including the roots themselves.
 *
 * Walks the loaded `children` of each node depth-first; nodes whose children have
 * not been fetched yet still contribute their own urn (they are expandable), we
 * just can't see below them until they load. Drives section-level collapse-all
 * (which urns to clear) and the expand-state check below.
 */
export function collectExpandableUrns(roots: DocumentTreeNode[]): string[] {
    const urns: string[] = [];
    const walk = (node: DocumentTreeNode) => {
        if (node.hasChildren) urns.push(node.urn);
        (node.children || []).forEach(walk);
    };
    roots.forEach(walk);
    return urns;
}

/**
 * True when any expandable node under `roots` is currently expanded. Drives the
 * section toggle glyph: collapse-all when something is open, expand-all otherwise.
 */
export function hasExpandedDescendant(roots: DocumentTreeNode[], expandedUrns: Set<string>): boolean {
    return collectExpandableUrns(roots).some((urn) => expandedUrns.has(urn));
}

interface ExpandAllFoldersArgs {
    roots: DocumentTreeNode[];
    loadChildren: (urn: string) => Promise<DocumentTreeNode[]>;
    onExpandLevel: (urns: string[]) => void;
    /** When present, reuse already-loaded children instead of refetching. */
    getLoadedChildren?: (urn: string) => DocumentTreeNode[] | undefined;
    maxDepth?: number;
    maxFolders?: number;
    concurrency?: number;
}

export type ExpandAllFoldersResult = {
    truncated: boolean;
    foldersExpanded: number;
};

/** Run `fn` over `items` with at most `concurrency` in flight. */
export async function mapPool<T, R>(items: T[], concurrency: number, fn: (item: T) => Promise<R>): Promise<R[]> {
    if (items.length === 0) return [];
    const results: R[] = new Array(items.length);
    let nextIndex = 0;

    const worker = async () => {
        while (nextIndex < items.length) {
            const index = nextIndex;
            nextIndex += 1;
            // Sequential per worker so we never exceed `concurrency` in flight.
            // eslint-disable-next-line no-await-in-loop
            results[index] = await fn(items[index]);
        }
    };

    const poolSize = Math.min(Math.max(1, concurrency), items.length);
    await Promise.all(Array.from({ length: poolSize }, () => worker()));
    return results;
}

/**
 * Breadth-first expand of folders under `roots`, capped by depth, folder count,
 * and fetch concurrency so large libraries cannot freeze the sidebar.
 */
export async function expandAllFolders({
    roots,
    loadChildren,
    onExpandLevel,
    getLoadedChildren,
    maxDepth = EXPAND_ALL_MAX_DEPTH,
    maxFolders = EXPAND_ALL_MAX_FOLDERS,
    concurrency = EXPAND_ALL_CONCURRENCY,
}: ExpandAllFoldersArgs): Promise<ExpandAllFoldersResult> {
    const seen = new Set<string>();
    let foldersExpanded = 0;
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

    const resolveChildren = async (urn: string): Promise<DocumentTreeNode[]> => {
        const loaded = getLoadedChildren?.(urn);
        if (loaded !== undefined) return loaded;
        return loadChildren(urn);
    };

    let current = enqueue(roots.filter((node) => node.hasChildren).map((node) => node.urn));
    let depth = 0;

    while (current.length > 0) {
        if (depth >= maxDepth) {
            truncated = true;
            break;
        }

        const remaining = maxFolders - foldersExpanded;
        if (remaining <= 0) {
            truncated = true;
            break;
        }

        if (current.length > remaining) {
            truncated = true;
            current = current.slice(0, remaining);
        }

        onExpandLevel(current);
        foldersExpanded += current.length;

        // eslint-disable-next-line no-await-in-loop
        const results = await mapPool(current, concurrency, resolveChildren);
        const candidates: string[] = [];
        results.forEach((children) => {
            children.forEach((child) => {
                if (child.hasChildren) candidates.push(child.urn);
            });
        });

        depth += 1;
        current = enqueue(candidates);
        if (current.length > 0 && (depth >= maxDepth || foldersExpanded >= maxFolders)) {
            truncated = true;
            current = [];
        }
    }

    return { truncated, foldersExpanded };
}
