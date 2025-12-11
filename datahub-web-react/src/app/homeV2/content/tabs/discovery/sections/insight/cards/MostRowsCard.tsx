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
    buildMostRowsFilters,
    buildMostRowsSort,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetMostRows';

import { EntityType } from '@types';

export const MOST_ROWS_ID = 'MostRows';

export const MostRowsCard = () => {
    return (
        <SearchListInsightCard
            id={MOST_ROWS_ID}
            title="Largest Tables by Rows"
            types={[EntityType.Dataset]}
            filters={buildMostRowsFilters()}
            sort={buildMostRowsSort()}
        />
    );
};
