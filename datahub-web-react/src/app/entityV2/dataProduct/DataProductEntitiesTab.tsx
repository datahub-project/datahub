/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import generateUseListDataProductAssets from '@app/entityV2/dataProduct/generateUseListDataProductAssets';
import { generateUseListDataProductAssetsCount } from '@app/entityV2/dataProduct/generateUseListDataProductAssetsCount';
import { SearchCardContext } from '@app/entityV2/shared/SearchCardContext';
import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';

export function DataProductEntitiesTab() {
    const { urn } = useEntityData();

    return (
        <SearchCardContext.Provider value={{ showRemovalFromList: true }}>
            <EmbeddedListSearchSection
                useGetSearchResults={generateUseListDataProductAssets({ urn })}
                useGetSearchCountResult={generateUseListDataProductAssetsCount({ urn })}
                emptySearchQuery="*"
                placeholderText="Filter assets..."
                skipCache
                applyView
            />
        </SearchCardContext.Provider>
    );
}
