import React from 'react';

interface LineageGraphContextType {
    isDAGView: boolean;
}

export default React.createContext<LineageGraphContextType>({ isDAGView: false });
