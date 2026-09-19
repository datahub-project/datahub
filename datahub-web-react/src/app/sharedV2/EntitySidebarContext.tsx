import React from 'react';

export interface FineGrainedOperation {
    inputColumns?: Array<[string, string]>; // [table, column]
    outputColumns?: Array<[string, string]>;
    transformOperation?: string;
}

export type SearchResultLineageCounts = {
    upstream?: { filtered?: number | null; total?: number | null } | null;
    downstream?: { filtered?: number | null; total?: number | null } | null;
};

interface EntitySidebarContextProps {
    width: number;
    setSidebarClosed: (isClosed: boolean) => void;
    isClosed: boolean;
    fineGrainedOperations?: FineGrainedOperation[]; // For query entities in lineage, when a column is selected
    forLineage?: boolean;
    separateSiblings?: boolean;
    searchResultLineage?: SearchResultLineageCounts | null;
}

export const entitySidebarContextDefaults: EntitySidebarContextProps = {
    width: window.innerWidth * 0.3,
    setSidebarClosed: () => {},
    isClosed: false,
};

const EntitySidebarContext = React.createContext<EntitySidebarContextProps>({
    ...entitySidebarContextDefaults,
});

export default EntitySidebarContext;
