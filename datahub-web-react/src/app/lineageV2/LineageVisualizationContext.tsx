import React from 'react';

type Urn = string;

interface VisualizationContext {
    searchQuery: string;
    setSearchQuery: (query: string) => void;
    searchedEntity: Urn | null;
    setSearchedEntity: (entity: Urn | null) => void;
}

const LineageVisualizationContext = React.createContext<VisualizationContext>({
    searchQuery: '',
    setSearchQuery: () => {},
    searchedEntity: null,
    setSearchedEntity: () => {},
});

export default LineageVisualizationContext;
