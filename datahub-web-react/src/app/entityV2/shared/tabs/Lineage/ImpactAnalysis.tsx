import React from 'react';

import { EmbeddedListSearchEmbed } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchEmbed';
import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import { FilterSet } from '@app/entityV2/shared/components/styled/search/types';
import generateUseDownloadScrollAcrossLineageSearchResultsHook from '@app/entityV2/shared/tabs/Lineage/generateUseDownloadScrollAcrossLineageSearchResultsHook';
import generateUseSearchResultsViaRelationshipHook, {
    generateUseSearchResultsCountViaRelationshipHook,
} from '@app/entityV2/shared/tabs/Lineage/generateUseSearchResultsViaRelationshipHook';

import { FacetFilterInput, LineageDirection } from '@types';

type Props = {
    type?: 'default' | 'compact';
    urn: string;
    direction: LineageDirection;
    shouldRefetch?: boolean;
    startTimeMillis?: number;
    endTimeMillis?: number;
    skipCache?: boolean;
    defaultShowFilters?: boolean;
    fixedFilters?: FilterSet;
    defaultFilters?: Array<FacetFilterInput>;
    showFilterBar?: boolean;
    setSkipCache?: (skipCache: boolean) => void;
    resetShouldRefetch?: () => void;
    setIsLoading?: React.Dispatch<React.SetStateAction<boolean>>;
};

export const ImpactAnalysis = ({
    type = 'default',
    urn,
    direction,
    startTimeMillis,
    endTimeMillis,
    shouldRefetch,
    skipCache,
    defaultShowFilters = true,
    fixedFilters,
    defaultFilters,
    showFilterBar = true,
    setSkipCache,
    resetShouldRefetch,
    setIsLoading,
}: Props) => {
    const finalStartTimeMillis = startTimeMillis || undefined;
    const finalEndTimeMillis = endTimeMillis || undefined;

    const Component = type === 'default' ? EmbeddedListSearchSection : EmbeddedListSearchEmbed;

    return (
        <Component
            useGetSearchResults={generateUseSearchResultsViaRelationshipHook({
                urn,
                direction,
                startTimeMillis: finalStartTimeMillis,
                endTimeMillis: finalEndTimeMillis,
                skipCache,
                setSkipCache,
                setIsLoading,
            })}
            useGetDownloadSearchResults={generateUseDownloadScrollAcrossLineageSearchResultsHook({
                urn,
                direction,
                startTimeMillis: finalStartTimeMillis,
                endTimeMillis: finalEndTimeMillis,
                skipCache,
                setSkipCache,
            })}
            useGetSearchCountResult={generateUseSearchResultsCountViaRelationshipHook({
                urn,
                direction,
                startTimeMillis: finalStartTimeMillis,
                endTimeMillis: finalEndTimeMillis,
                skipCache,
                setSkipCache,
            })}
            defaultShowFilters={defaultShowFilters}
            fixedFilters={fixedFilters}
            defaultFilters={defaultFilters || [{ field: 'degree', values: ['1'] }]}
            shouldRefetch={shouldRefetch}
            resetShouldRefetch={resetShouldRefetch}
            placeholderText="Search related assets..."
            showFilterBar={showFilterBar}
            applyView
        />
    );
};
