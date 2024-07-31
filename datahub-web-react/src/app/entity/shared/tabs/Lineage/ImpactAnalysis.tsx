import React from 'react';
import { LineageDirection } from '../../../../../types.generated';
import generateUseSearchResultsViaRelationshipHook from './generateUseSearchResultsViaRelationshipHook';
import { EmbeddedListSearchSection } from '../../components/styled/search/EmbeddedListSearchSection';
import generateUseDownloadScrollAcrossLineageSearchResultsHook from './generateUseDownloadScrollAcrossLineageSearchResultsHook';

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
