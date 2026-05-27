import React from 'react';

type Urn = string;

interface VisualizationContext {
    searchQuery: string;
    setSearchQuery: (query: string) => void;
    searchedEntity: Urn | null;
    setSearchedEntity: (entity: Urn | null) => void;
    isFocused: boolean;
    /**
     * Ephemeral pulse — set to suspend virt for the duration of a screenshot
     * capture so html-to-image walks a fully-mounted DOM. Always restore to
     * `false` after the capture; leaving it on defeats virt at scale.
     */
    forceMountAll: boolean;
    setForceMountAll: (value: boolean) => void;
}

const LineageVisualizationContext = React.createContext<VisualizationContext>({
    searchQuery: '',
    setSearchQuery: () => {},
    searchedEntity: null,
    setSearchedEntity: () => {},
    isFocused: false,
    forceMountAll: false,
    setForceMountAll: () => {},
});

export default LineageVisualizationContext;
