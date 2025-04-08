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
