import { CaretRight } from '@phosphor-icons/react';
import React, { useMemo, useRef } from 'react';
import styled from 'styled-components';

import { MoreFilterOptionLabel } from '@app/searchV2/filters/styledComponents';
import { FilterPredicate } from '@app/searchV2/filters/types';
import useSearchFilterDropdown from '@app/searchV2/filters/useSearchFilterDropdown';
import { useElementDimensions, useFilterDisplayName } from '@app/searchV2/filters/utils';
import ValueSelector from '@app/searchV2/filters/value/ValueSelector';

import { FacetFilterInput, FacetMetadata } from '@types';

const OptionDisplayName = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: inherit;
`;

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    filterPredicates: FilterPredicate[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function MoreFilterOption({ filter, filterPredicates, activeFilters, onChangeFilters }: Props) {
    const { finalAggregations, updateFilters, numActiveFilters, manuallyUpdateFilters } = useSearchFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
    });
    const displayName = useFilterDisplayName(filter);
    const labelRef = useRef<HTMLDivElement>(null);
    const { width, height, isElementOutsideWindow } = useElementDimensions(labelRef);

    const menuStyle = useMemo(
        () => ({
            top: -height,
            left: isElementOutsideWindow ? undefined : width,
            right: isElementOutsideWindow ? width : undefined,
        }),
        [width, height, isElementOutsideWindow],
    );

    const onChangeFilterValues = (currentFilterPredicate: FilterPredicate, newValues) => {
        if (currentFilterPredicate.values !== newValues) {
            updateFilters(newValues);
        }
    };

    const currentFilterPredicate = filterPredicates?.find((obj) =>
        obj.field.field.includes(filter.field),
    ) as FilterPredicate;

    return (
        <ValueSelector
            field={currentFilterPredicate?.field}
            values={currentFilterPredicate?.values}
            defaultOptions={finalAggregations}
            onChangeValues={(newValues) => onChangeFilterValues(currentFilterPredicate, newValues)}
            manuallyUpdateFilters={manuallyUpdateFilters}
            menuStyle={menuStyle}
        >
            <MoreFilterOptionLabel
                $isActive={!!numActiveFilters}
                data-testid={`more-filter-${displayName?.replace(/\s/g, '-')}`}
                ref={labelRef}
            >
                <OptionDisplayName>
                    {displayName} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                </OptionDisplayName>
                <CaretRight size={12} />
            </MoreFilterOptionLabel>
        </ValueSelector>
    );
}
