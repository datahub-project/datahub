import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import generateUseListApplicationAssets from '@app/entityV2/application/generateUseListApplicationAssets';
import { generateUseListApplicationAssetsCount } from '@app/entityV2/application/generateUseListApplicationAssetsCount';
import { SearchCardContext } from '@app/entityV2/shared/SearchCardContext';
import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';

export function ApplicationEntitiesTab() {
    const { urn } = useEntityData();

    return (
        <SearchCardContext.Provider value={{ showRemovalFromList: true }}>
            <EmbeddedListSearchSection
                useGetSearchResults={generateUseListApplicationAssets({ urn })}
                useGetSearchCountResult={generateUseListApplicationAssetsCount({ urn })}
                emptySearchQuery="*"
                placeholderText="Filter assets..."
                skipCache
                applyView
            />
        </SearchCardContext.Provider>
    );
}
