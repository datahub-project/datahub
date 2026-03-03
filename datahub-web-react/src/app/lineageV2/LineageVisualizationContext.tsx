import React from 'react';

type Urn = string;

interface VisualizationContext {
    searchQuery: string;
    setSearchQuery: (query: string) => void;
    searchedEntity: Urn | null;
    setSearchedEntity: (entity: Urn | null) => void;
    isFocused: boolean;
}

const LineageVisualizationContext = React.createContext<VisualizationContext>({
    searchQuery: '',
    setSearchQuery: () => {},
    searchedEntity: null,
    setSearchedEntity: () => {},
    isFocused: false,
});

export default LineageVisualizationContext;
