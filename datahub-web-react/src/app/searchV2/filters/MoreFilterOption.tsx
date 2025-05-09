import { RightOutlined } from '@ant-design/icons';
import React, { useRef } from 'react';
import styled from 'styled-components';

import { IconWrapper } from '@app/searchV2/filters/SearchFilterView';
import { MoreFilterOptionLabel } from '@app/searchV2/filters/styledComponents';
import { FilterPredicate } from '@app/searchV2/filters/types';
import useSearchFilterDropdown from '@app/searchV2/filters/useSearchFilterDropdown';
import { getFilterDropdownIcon, useElementDimensions, useFilterDisplayName } from '@app/searchV2/filters/utils';
import ValueSelector from '@app/searchV2/filters/value/ValueSelector';

import { FacetFilterInput, FacetMetadata } from '@types';

const IconNameWrapper = styled.span`
    display: flex;
    align-items: center;
`;

const StyledValueSelector = styled(ValueSelector)<{ width: number; height: number; isElementOutsideWindow: boolean }>`
    position: absolute;
    top: -${(props) => props.height}px;
    ${(props) => (props.isElementOutsideWindow ? 'right' : 'left')}: ${(props) => props.width}px;
`;

const StyledRightOutlined = styled(RightOutlined)`
    font-size: 12px;
    height: 12px;
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
    const filterIcon = getFilterDropdownIcon(filter.field);
    const labelRef = useRef<HTMLDivElement>(null);
    const elementDimensions = useElementDimensions(labelRef);

    const onChangeFilterValues = (currentFilterPredicate: FilterPredicate, newValues) => {
        if (currentFilterPredicate.values !== newValues) {
            updateFilters(newValues);
        }
    };

    const currentFilterPredicate = filterPredicates?.find((obj) =>
        obj.field.field.includes(filter.field),
    ) as FilterPredicate;

    return (
        <StyledValueSelector
            field={currentFilterPredicate?.field}
            values={currentFilterPredicate?.values}
            defaultOptions={finalAggregations}
            onChangeValues={(newValues) => onChangeFilterValues(currentFilterPredicate, newValues)}
            manuallyUpdateFilters={manuallyUpdateFilters}
            {...elementDimensions}
        >
            <MoreFilterOptionLabel
                $isActive={!!numActiveFilters}
                data-testid={`more-filter-${displayName?.replace(/\s/g, '-')}`}
                ref={labelRef}
            >
                <IconNameWrapper>
                    {filterIcon && <IconWrapper>{filterIcon}</IconWrapper>}
                    {displayName} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                </IconNameWrapper>
                <StyledRightOutlined />
            </MoreFilterOptionLabel>
        </StyledValueSelector>
    );
}
