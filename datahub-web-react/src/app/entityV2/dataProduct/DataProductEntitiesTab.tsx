import React from 'react';
import { useEntityData } from '../../entity/shared/EntityContext';
import { EmbeddedListSearchSection } from '../shared/components/styled/search/EmbeddedListSearchSection';
import generateUseListDataProductAssets from './generateUseListDataProductAssets';
import { SearchCardContext } from '../shared/SearchCardContext';

export function DataProductEntitiesTab() {
    const { urn } = useEntityData();

    return (
        <SearchCardContext.Provider value={{ showRemovalFromList: true }}>
            <EmbeddedListSearchSection
                useGetSearchResults={generateUseListDataProductAssets({ urn })}
                emptySearchQuery="*"
                placeholderText="Filter assets..."
                skipCache
            />
        </SearchCardContext.Provider>
    );
}
