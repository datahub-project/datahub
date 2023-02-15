import React from 'react';
import { LineageDirection } from '../../../../../types.generated';
import generateUseSearchResultsViaRelationshipHook from './generateUseSearchResultsViaRelationshipHook';
import { EmbeddedListSearchSection } from '../../components/styled/search/EmbeddedListSearchSection';
import { useGetTimeParams } from '../../../../lineage/utils/useGetTimeParams';

type Props = {
    urn: string;
    direction: LineageDirection;
    shouldRefetch?: boolean;
    resetShouldRefetch?: () => void;
};

export const ImpactAnalysis = ({ urn, direction, shouldRefetch, resetShouldRefetch }: Props) => {
    const { startTimeMillis, endTimeMillis } = useGetTimeParams();

    return (
        <EmbeddedListSearchSection
            useGetSearchResults={generateUseSearchResultsViaRelationshipHook({
                urn,
                direction,
                startTimeMillis: startTimeMillis || undefined,
                endTimeMillis: endTimeMillis || undefined,
            })}
            defaultShowFilters
            defaultFilters={[{ field: 'degree', values: ['1'] }]}
            shouldRefetch={shouldRefetch}
            resetShouldRefetch={resetShouldRefetch}
        />
    );
};
