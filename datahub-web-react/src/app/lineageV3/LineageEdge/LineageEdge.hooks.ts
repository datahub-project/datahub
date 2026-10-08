import { useContext, useMemo } from 'react';

import { LineageDisplayContext } from '@app/lineageV3/common';

interface EdgeHighlight {
    isHighlighted: boolean;
    /** Stroke to draw a column-highlighted edge with, matching the column edges it continues. */
    highlightStroke: string | undefined;
    /** Whether to fade the edge because a column is selected and this edge is not part of it. */
    isColumnSelected: boolean;
    /** Whether anything is highlighted, i.e. unhighlighted edges should recede. */
    anyHighlighted: boolean;
}

/**
 * Highlight state of an entity edge. Hovering a node highlights every edge reachable from it;
 * separately, a column highlight covers the entity edges between column-like entities such as
 * metrics, which stay highlighted while a column is selected.
 */
export default function useEdgeHighlight(id: string, originalId: string): EdgeHighlight {
    const { selectedColumn, highlightedEdges, columnHighlightedEdges } = useContext(LineageDisplayContext);

    return useMemo(() => {
        const highlightStroke = columnHighlightedEdges.get(id) ?? columnHighlightedEdges.get(originalId);
        return {
            isHighlighted:
                !!highlightStroke ||
                (!selectedColumn && (highlightedEdges.has(id) || highlightedEdges.has(originalId))),
            highlightStroke,
            isColumnSelected: !!selectedColumn && !highlightStroke,
            anyHighlighted: !!highlightedEdges.size,
        };
    }, [id, originalId, selectedColumn, highlightedEdges, columnHighlightedEdges]);
}
