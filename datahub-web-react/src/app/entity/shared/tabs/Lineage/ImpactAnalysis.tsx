/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { EmbeddedListSearchSection } from '@app/entity/shared/components/styled/search/EmbeddedListSearchSection';
import generateUseDownloadScrollAcrossLineageSearchResultsHook from '@app/entity/shared/tabs/Lineage/generateUseDownloadScrollAcrossLineageSearchResultsHook';
import generateUseSearchResultsViaRelationshipHook from '@app/entity/shared/tabs/Lineage/generateUseSearchResultsViaRelationshipHook';

import { LineageDirection } from '@types';

type Props = {
    urn: string;
    direction: LineageDirection;
    shouldRefetch?: boolean;
    startTimeMillis?: number;
    endTimeMillis?: number;
    skipCache?: boolean;
    setSkipCache?: (skipCache: boolean) => void;
    resetShouldRefetch?: () => void;
    onLineageClick?: () => void;
    isLineageTab?: boolean;
};

export const ImpactAnalysis = ({
    urn,
    direction,
    startTimeMillis,
    endTimeMillis,
    shouldRefetch,
    skipCache,
    setSkipCache,
    resetShouldRefetch,
    onLineageClick,
    isLineageTab,
}: Props) => {
    const finalStartTimeMillis = startTimeMillis || undefined;
    const finalEndTimeMillis = endTimeMillis || undefined;
    return (
        <EmbeddedListSearchSection
            useGetSearchResults={generateUseSearchResultsViaRelationshipHook({
                urn,
                direction,
                startTimeMillis: finalStartTimeMillis,
                endTimeMillis: finalEndTimeMillis,
                skipCache,
                setSkipCache,
            })}
            useGetDownloadSearchResults={generateUseDownloadScrollAcrossLineageSearchResultsHook({
                urn,
                direction,
                startTimeMillis: finalStartTimeMillis,
                endTimeMillis: finalEndTimeMillis,
                skipCache,
                setSkipCache,
            })}
            defaultShowFilters
            defaultFilters={[{ field: 'degree', values: ['1'] }]}
            shouldRefetch={shouldRefetch}
            resetShouldRefetch={resetShouldRefetch}
            onLineageClick={onLineageClick}
            isLineageTab={isLineageTab}
        />
    );
};
