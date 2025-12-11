/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { isEqual } from 'lodash';

import { ColumnEdge } from '@app/lineage/types';

function highlightDownstreamColumnLineage(
    sourceField: string,
    sourceUrn: string,
    edges: ColumnEdge[],
    fineGrainedMap: any,
) {
    const forwardLineage = fineGrainedMap.forward[sourceUrn]?.[sourceField];
    if (forwardLineage) {
        Object.entries(forwardLineage).forEach((entry) => {
            const [targetUrn, fieldPaths] = entry;
            (fieldPaths as string[]).forEach((targetField) => {
                const edge: ColumnEdge = { sourceUrn, sourceField, targetUrn, targetField };
                if (!edges.some((value) => isEqual(value, edge))) {
                    edges.push(edge);
                    highlightDownstreamColumnLineage(targetField, targetUrn, edges, fineGrainedMap);
                }
            });
        });
    }
}

function highlightUpstreamColumnLineage(
    targetField: string,
    targetUrn: string,
    edges: ColumnEdge[],
    fineGrainedMap: any,
) {
    const reverseLineage = fineGrainedMap.reverse[targetUrn]?.[targetField];
    if (reverseLineage) {
        Object.entries(reverseLineage).forEach((entry) => {
            const [sourceUrn, fieldPaths] = entry;
            (fieldPaths as string[]).forEach((sourceField) => {
                const edge: ColumnEdge = { sourceUrn, sourceField, targetUrn, targetField };
                if (!edges.some((value) => isEqual(value, edge))) {
                    edges.push(edge);
                    highlightUpstreamColumnLineage(sourceField, sourceUrn, edges, fineGrainedMap);
                }
            });
        });
    }
}

export function highlightColumnLineage(
    fieldPath: string,
    fineGrainedMap: any,
    urn: string,
    setHighlightedEdges: (edgesToHighlight: ColumnEdge[]) => void,
) {
    const edgesToHighlight: ColumnEdge[] = [];
    if (urn) {
        highlightDownstreamColumnLineage(fieldPath, urn, edgesToHighlight, fineGrainedMap);
        highlightUpstreamColumnLineage(fieldPath, urn, edgesToHighlight, fineGrainedMap);
    }
    setHighlightedEdges(edgesToHighlight);
}
