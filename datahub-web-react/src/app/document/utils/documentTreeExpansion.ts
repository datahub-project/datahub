import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

/**
 * Collect the urns of every expandable node (a node that `hasChildren`) reachable
 * from `roots` through the already-loaded tree, including the roots themselves.
 *
 * Walks the loaded `children` of each node depth-first; nodes whose children have
 * not been fetched yet still contribute their own urn (they are expandable), we
 * just can't see below them until they load. Drives section-level collapse-all
 * (which urns to clear) and the expand-state check below.
 *
 * @param roots - The section's root nodes to walk
 * @returns Urns of expandable nodes in depth-first order
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
 *
 * @param roots - The section's root nodes to walk
 * @param expandedUrns - The set of currently-expanded node urns
 */
export function hasExpandedDescendant(roots: DocumentTreeNode[], expandedUrns: Set<string>): boolean {
    return collectExpandableUrns(roots).some((urn) => expandedUrns.has(urn));
}

interface ExpandAllFoldersArgs {
    /** The section's root nodes to expand from. */
    roots: DocumentTreeNode[];
    /** Loads (and returns) the freshly-fetched children of a parent urn. */
    loadChildren: (urn: string) => Promise<DocumentTreeNode[]>;
    /** Invoked with each breadth-first level's urns so the caller can expand them. */
    onExpandLevel: (urns: string[]) => void;
}

/**
 * Breadth-first expand of every folder under `roots`, loading children one level
 * at a time.
 *
 * `loadChildren` returns the nodes it just loaded, so we discover the next level
 * of folders from its return value rather than reading back potentially-stale
 * tree state. `onExpandLevel` fires per level so expansion state updates as the
 * walk progresses (rather than in one batch at the end). A `seen` set makes the
 * walk resilient to repeats/cycles and prevents redundant fetches.
 *
 * @returns Resolves once the whole reachable folder subtree has been expanded.
 */
export async function expandAllFolders({ roots, loadChildren, onExpandLevel }: ExpandAllFoldersArgs): Promise<void> {
    const seen = new Set<string>();

    // Keep only urns we haven't queued before, marking them seen as we go. This
    // dedupes both within a level (two parents sharing a child) and across levels
    // (a folder reachable by more than one path), so each folder loads/expands once.
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

    let current = enqueue(roots.filter((node) => node.hasChildren).map((node) => node.urn));

    while (current.length > 0) {
        onExpandLevel(current);

        // eslint-disable-next-line no-await-in-loop
        const results = await Promise.all(current.map((urn) => loadChildren(urn)));
        const candidates: string[] = [];
        results.forEach((children) => {
            children.forEach((child) => {
                if (child.hasChildren) candidates.push(child.urn);
            });
        });
        current = enqueue(candidates);
    }
}
