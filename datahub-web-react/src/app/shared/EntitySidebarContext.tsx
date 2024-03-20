import React from 'react';

export interface ColumnQueryData {
    inputColumns?: Array<[string, string]>;
    outputColumns?: Array<[string, string]>;
    transformOperation?: string;
}

export interface EntitySidebarQueryDetails extends ColumnQueryData {
    inputTables: string[];
    outputTables: string[];
}

interface EntitySidebarContextProps {
    width?: number;
    setSidebarClosed: (isClosed: boolean) => void;
    isClosed: boolean;
    extra?: EntitySidebarQueryDetails;
    forLineage?: boolean;
}

const EntitySidebarContext = React.createContext<EntitySidebarContextProps>({
    setSidebarClosed: () => {},
    isClosed: false,
});

export default EntitySidebarContext;
