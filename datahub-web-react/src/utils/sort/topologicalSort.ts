/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DataJob } from '@types';

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
