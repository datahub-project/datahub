import React from 'react';

import generateUseListDataProductAssets from '@app/entity/dataProduct/generateUseListDataProductAssets';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { EmbeddedListSearchSection } from '@app/entity/shared/components/styled/search/EmbeddedListSearchSection';

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
