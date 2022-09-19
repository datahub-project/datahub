import React from 'react';

export const LineageExplorerContext = React.createContext<LineageExplorerContextType>({
    expandTitles: false,
    showColumns: false,
    collapsedColumnsNodes: null,
    setCollapsedColumnsNodes: null,
    hoveredField: null,
    setHoveredField: null,
    fineGrainedMap: { forward: [], reverse: [] },
});

type LineageExplorerContextType = {
    expandTitles: boolean;
    showColumns: boolean;
    collapsedColumnsNodes: any;
    setCollapsedColumnsNodes: any;
    hoveredField: any;
    setHoveredField: any;
    fineGrainedMap: any;
};
