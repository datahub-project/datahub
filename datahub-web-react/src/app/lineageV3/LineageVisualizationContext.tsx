import React from 'react';

type Urn = string;

interface VisualizationContext {
    searchQuery: string;
    setSearchQuery: (query: string) => void;
    searchedEntity: Urn | null;
    setSearchedEntity: (entity: Urn | null) => void;
    isFocused: boolean;
    isDraggingBoundingBox: boolean; // Used to prevent transition when dragging bounding boxes
    setIsDraggingBoundingBox: (dragging: boolean) => void;
}

const LineageVisualizationContext = React.createContext<VisualizationContext>({
    searchQuery: '',
    setSearchQuery: () => {},
    searchedEntity: null,
    setSearchedEntity: () => {},
    isFocused: false,
    isDraggingBoundingBox: false,
    setIsDraggingBoundingBox: () => {},
});

export default LineageVisualizationContext;
