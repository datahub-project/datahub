import React from 'react';
import { LineageDirection } from '../../../../../types.generated';

export const LineageTabContext = React.createContext<LineageTabContextType>({
    lineageDirection: LineageDirection.Downstream,
    selectedColumn: undefined,
});

type LineageTabContextType = {
    lineageDirection: LineageDirection;
    selectedColumn?: string;
};
