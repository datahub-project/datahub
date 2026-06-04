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
    /**
     * Temporary escape hatch for code paths that need every node mounted
     * (e.g. screenshot export). When `true`, virtualisation should treat
     * every node as visible. Mirrors V2's `forceMountAll`.
     */
    forceMountAll: boolean;
    setForceMountAll: (force: boolean) => void;
}

const LineageVisualizationContext = React.createContext<VisualizationContext>({
    searchQuery: '',
    setSearchQuery: () => {},
    searchedEntity: null,
    setSearchedEntity: () => {},
    isFocused: false,
    isDraggingBoundingBox: false,
    setIsDraggingBoundingBox: () => {},
    forceMountAll: false,
    setForceMountAll: () => {},
});

export default LineageVisualizationContext;
