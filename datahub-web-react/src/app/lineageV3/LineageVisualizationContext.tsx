/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
