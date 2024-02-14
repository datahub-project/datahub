import React, { useContext } from 'react';
import { BezierEdge, EdgeProps } from 'reactflow';
import { LineageDisplayContext } from '../common';
import { LINEAGE_COLORS } from '../../entityV2/shared/constants';

export const LINEAGE_TABLE_EDGE_NAME = 'table-table';

export function LineageTableEdge(props: EdgeProps) {
    const { selectedColumn, highlightedEdges } = useContext(LineageDisplayContext);

    const stroke = !selectedColumn && highlightedEdges.has(props.id) ? LINEAGE_COLORS.BLUE_2 : undefined;
    const strokeOpacity = selectedColumn ? 0.5 : 1;
    return <BezierEdge {...props} style={{ stroke, strokeOpacity }} />;
}
