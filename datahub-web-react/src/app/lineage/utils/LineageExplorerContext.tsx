import React from 'react';

import { ColumnEdge } from '@app/lineage/types';

import { SchemaField, SchemaFieldRef } from '@types';

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
    visibleColumnsByUrn: {},
    setVisibleColumnsByUrn: () => {},
    columnsByUrn: {},
    setColumnsByUrn: () => {},
    refetchCenterNode: () => {},
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
    visibleColumnsByUrn: any;
    setVisibleColumnsByUrn: any;
    columnsByUrn: Record<string, SchemaField[]>;
    setColumnsByUrn: React.Dispatch<React.SetStateAction<Record<string, SchemaField[]>>>;
    refetchCenterNode: () => void;
};
