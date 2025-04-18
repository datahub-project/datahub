import { DataJob } from '../../types.generated';

function topologicalSortVisit(nodeUrn: string, result: string[], visited: string[], nodes: DataJob[]) {
    if (result.indexOf(nodeUrn) !== -1 || visited.indexOf(nodeUrn) !== -1) {
        return;
    }

    visited.push(nodeUrn);

    const node = nodes.find((materializedNode) => materializedNode.urn === nodeUrn);

    if (!node) {
        return;
    }

    node.inputOutput?.inputDatajobs?.forEach((inputDatajob) =>
        topologicalSortVisit(inputDatajob.urn, result, visited, nodes),
    );

    result.push(nodeUrn);
}

export function topologicalSort(nodes: Array<DataJob>) {
    const result: string[] = [];
    const visited: string[] = [];

    nodes.forEach((node) => {
        if (result.indexOf(node.urn) === -1) {
            topologicalSortVisit(node.urn, result, visited, nodes);
        }
    });

    return result.map((resultUrn) => nodes.find((node) => node.urn === resultUrn));
}
