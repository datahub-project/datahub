/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
