import React from 'react';

interface LineageGraphContextType {
    isDAGView: boolean;
    isModuleView?: boolean;
}

const LineageGraphContext = React.createContext<LineageGraphContextType>({ isDAGView: false, isModuleView: false });

export default LineageGraphContext;
