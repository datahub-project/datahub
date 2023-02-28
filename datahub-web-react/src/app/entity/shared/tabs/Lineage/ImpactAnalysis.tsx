import React from 'react';
import { LineageDirection } from '../../../../../types.generated';
import generateUseSearchResultsViaRelationshipHook from './generateUseSearchResultsViaRelationshipHook';
import { EmbeddedListSearchSection } from '../../components/styled/search/EmbeddedListSearchSection';
import { getDefaultLineageEndTime, getDefaultLineageStartTime } from '../../../../lineage/utils/lineageUtils';

type Props = {
    urn: string;
    direction: LineageDirection;
    shouldRefetch?: boolean;
    startTimeMillis?: number;
    endTimeMillis?: number;
    resetShouldRefetch?: () => void;
};

export const ImpactAnalysis = ({
    urn,
    direction,
    startTimeMillis,
    endTimeMillis,
    shouldRefetch,
    resetShouldRefetch,
}: Props) => {
    const finalStartTimeMillis = (startTimeMillis === undefined && getDefaultLineageStartTime()) || startTimeMillis;
    const finalEndTimeMillis = (endTimeMillis === undefined && getDefaultLineageEndTime()) || endTimeMillis;
    return (
        <EmbeddedListSearchSection
            useGetSearchResults={generateUseSearchResultsViaRelationshipHook({
                urn,
                direction,
                startTimeMillis: finalStartTimeMillis,
                endTimeMillis: finalEndTimeMillis,
            })}
            defaultShowFilters
            defaultFilters={[{ field: 'degree', values: ['1'] }]}
            shouldRefetch={shouldRefetch}
            resetShouldRefetch={resetShouldRefetch}
        />
    );
};
