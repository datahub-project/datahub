import React from 'react';
import { LineageDirection, LineageSearchPath } from '../../../../../types.generated';

export const LineageTabContext = React.createContext<LineageTabContextType>({
    isColumnLevelLineage: false,
    lineageDirection: LineageDirection.Downstream,
    selectedColumn: undefined,
    lineageSearchPath: null,
    setLineageSearchPath: () => {},
});

type LineageTabContextType = {
    isColumnLevelLineage: boolean;
    lineageDirection: LineageDirection;
    selectedColumn?: string;
    lineageSearchPath: LineageSearchPath | null;
    setLineageSearchPath: (path: LineageSearchPath | null) => void;
};
