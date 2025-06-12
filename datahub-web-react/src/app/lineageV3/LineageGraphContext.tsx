import React from 'react';

export interface LineageGraphContextType {
    isDAGView: boolean;
}

export default React.createContext<LineageGraphContextType>({ isDAGView: false });
