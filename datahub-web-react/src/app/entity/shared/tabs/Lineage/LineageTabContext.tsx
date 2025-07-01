import React from 'react';

import { LineageDirection } from '@types';

export const LineageTabContext = React.createContext<LineageTabContextType>({
    isColumnLevelLineage: false,
    lineageDirection: LineageDirection.Downstream,
    selectedColumn: undefined,
});

type LineageTabContextType = {
    isColumnLevelLineage: boolean;
    lineageDirection: LineageDirection;
    selectedColumn?: string;
};
