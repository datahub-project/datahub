/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { SearchListInsightCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/SearchListInsightCard';
import {
    buildFoundationalAssetsFilters,
    buildFoundationalAssetsSort,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetFoundationalAssets';
import { ASSET_ENTITY_TYPES } from '@app/searchV2/utils/constants';

export const FoundationalAssetsCard = () => {
    return (
        <SearchListInsightCard
            id="FoundationalAssets"
            types={[...ASSET_ENTITY_TYPES] as any}
            title="Foundational Assets"
            tip="Key data assets for your organization based popularity, freshness, and impact"
            filters={buildFoundationalAssetsFilters()}
            sort={buildFoundationalAssetsSort()}
        />
    );
};
