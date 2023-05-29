import React from 'react';
import { useEntityData } from '../shared/EntityContext';
import { EmbeddedListSearchSection } from '../shared/components/styled/search/EmbeddedListSearchSection';
import generateUseListDataProductAssets from './generateUseListDataProductAssets';

export function DataProductEntitiesTab() {
    const { urn } = useEntityData();

    return (
        <EmbeddedListSearchSection
            useGetSearchResults={generateUseListDataProductAssets({ urn })}
            emptySearchQuery="*"
            placeholderText="Filter assets..."
        />
    );
}
