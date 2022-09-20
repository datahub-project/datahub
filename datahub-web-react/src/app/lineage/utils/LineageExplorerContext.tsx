import React from 'react';
import { SchemaFieldRef } from '../../../types.generated';
import { ColumnEdge } from '../types';

export const LineageExplorerContext = React.createContext<LineageExplorerContextType>({
    expandTitles: false,
    showColumns: false,
    collapsedColumnsNodes: null,
    setCollapsedColumnsNodes: null,
    fineGrainedMap: { forward: [], reverse: [] },
    selectedField: null,
    setSelectedField: () => {},
    highlightedEdges: [],
    setHighlightedEdges: () => {},
});

type LineageExplorerContextType = {
    expandTitles: boolean;
    showColumns: boolean;
    collapsedColumnsNodes: any;
    setCollapsedColumnsNodes: any;
    fineGrainedMap: any;
    selectedField: SchemaFieldRef | null;
    setSelectedField: (field: SchemaFieldRef | null) => void;
    highlightedEdges: ColumnEdge[];
    setHighlightedEdges: React.Dispatch<React.SetStateAction<ColumnEdge[]>>;
};
