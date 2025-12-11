/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

// TODO: Replace `getTopologicalOrder` in useColumnHighlighting.ts
export default function topologicalSort(nodes: Set<string>, children: Map<string, Set<string>>): string[] {
    const topologicalOrder: string[] = [];

    const temporaryMarkedNodes = new Set<string>();
    const permanentMarkedNodes = new Set<string>();

    function visit(node: string) {
        if (permanentMarkedNodes.has(node)) return;
        if (temporaryMarkedNodes.has(node)) {
            console.debug(`Cycle detected in topological sort for node: ${node}`);
            return;
        }
        temporaryMarkedNodes.add(node);
        children.get(node)?.forEach((child) => {
            if (nodes.has(child) && !permanentMarkedNodes.has(child)) {
                visit(child);
            }
        });
        permanentMarkedNodes.add(node);
        topologicalOrder.push(node);
    }
    nodes.forEach(visit);

    topologicalOrder.reverse();
    return topologicalOrder;
}
