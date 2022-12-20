import React from 'react';
import { LineageDirection } from '../../../../../types.generated';
import generateUseSearchResultsViaRelationshipHook from './generateUseSearchResultsViaRelationshipHook';
import { EmbeddedListSearchSection } from '../../components/styled/search/EmbeddedListSearchSection';

type Props = {
    urn: string;
    direction: LineageDirection;
    shouldRefetch?: boolean;
    resetShouldRefetch?: () => void;
};

export const ImpactAnalysis = ({ urn, direction, shouldRefetch, resetShouldRefetch }: Props) => {
    return (
        <EmbeddedListSearchSection
            useGetSearchResults={generateUseSearchResultsViaRelationshipHook({
                urn,
                direction,
            })}
            defaultShowFilters
            defaultFilters={[{ field: 'degree', values: ['1'] }]}
            shouldRefetch={shouldRefetch}
            resetShouldRefetch={resetShouldRefetch}
        />
    );
};
