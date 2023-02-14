import { ColumnEdge } from '../types';

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
                edges.push({ sourceUrn, sourceField, targetUrn, targetField });
                highlightDownstreamColumnLineage(targetField, targetUrn, edges, fineGrainedMap);
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
                edges.push({ targetUrn, targetField, sourceUrn, sourceField });
                highlightUpstreamColumnLineage(sourceField, sourceUrn, edges, fineGrainedMap);
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
