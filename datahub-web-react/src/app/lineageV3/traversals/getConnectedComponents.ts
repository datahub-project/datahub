import { setDifference } from '@app/lineageV3/common';

/**
 * Treat the graph as undirected and return the connected components, by performing a depth-first search (DFS).
 * @returns A list of connected components, each represented as a set of node URNs.
 */
export default function getConnectedComponents(nodes: Set<string>, neighbors: Map<string, Set<string>>): Set<string>[] {
    const visited = new Set<string>();
    const components: Set<string>[] = [];

    // iterative dfs
    let startValue: string | undefined;
    // eslint-disable-next-line no-cond-assign
    while ((startValue = setDifference(nodes, visited).pop())) {
        const component = new Set<string>([startValue]);
        const stack = [startValue];
        let node: string | undefined;
        // eslint-disable-next-line no-cond-assign
        while ((node = stack.pop())) {
            if (!visited.has(node)) {
                visited.add(node);
                component.add(node);
                neighbors.get(node)?.forEach((n) => stack.push(n));
            }
        }

        components.push(component);
    }

    return components;
}
