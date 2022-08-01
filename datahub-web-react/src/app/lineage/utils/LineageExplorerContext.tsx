import React from 'react';

export const LineageExplorerContext = React.createContext<LineageExplorerContextType>({
    expandTitles: false,
    showColumns: false,
    expandedNodes: null,
    setExpandedNodes: null,
    hoveredField: null,
    setHoveredField: null,
    fineGrainedMap: { forward: [], reverse: [] },
});

type LineageExplorerContextType = {
    expandTitles: boolean;
    showColumns: boolean;
    expandedNodes: any;
    setExpandedNodes: any;
    hoveredField: any;
    setHoveredField: any;
    fineGrainedMap: any;
};
