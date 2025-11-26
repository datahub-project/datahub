import React from 'react';

export interface LineageGraphContextType {
    isDAGView: boolean;
    isModuleView?: boolean;
}

const LineageGraphContext = React.createContext<LineageGraphContextType>({ isDAGView: false, isModuleView: false });

export default LineageGraphContext;

export const useLineageGraphContext = () => React.useContext(LineageGraphContext);
