import { RightOutlined } from '@ant-design/icons';
import React, { useRef } from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import { IconWrapper } from './SearchFilterView';
import { MoreFilterOptionLabel } from './styledComponents';
import { FilterPredicate } from './types';
import useSearchFilterDropdown from './useSearchFilterDropdown';
import { getFilterDropdownIcon, useElementDimensions } from './utils';
import ValueSelector from './value/ValueSelector';

const IconNameWrapper = styled.span`
    display: flex;
    align-items: center;
`;

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    filterPredicates: FilterPredicate[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function MoreFilterOption({ filter, filterPredicates, activeFilters, onChangeFilters }: Props) {
    const { updateFilters, numActiveFilters } = useSearchFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
    });
    const filterIcon = getFilterDropdownIcon(filter.field);
    const labelRef = useRef<HTMLDivElement>(null);
    const { width, height, isElementOutsideWindow } = useElementDimensions(labelRef);

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
            defaultOptions={currentFilterPredicate?.defaultValueOptions}
            onChangeValues={(newValues) => onChangeFilterValues(currentFilterPredicate, newValues)}
            style={{
                position: 'absolute',
                top: -height,
                [isElementOutsideWindow ? 'right' : 'left']: width,
            }}
        >
            <MoreFilterOptionLabel
                $isActive={!!numActiveFilters}
                data-testid={`more-filter-${capitalizeFirstLetterOnly(filter.displayName)?.replace(/\s/g, '-')}`}
                ref={labelRef}
            >
                <IconNameWrapper>
                    {filterIcon && <IconWrapper>{filterIcon}</IconWrapper>}
                    {capitalizeFirstLetterOnly(filter.displayName)} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                </IconNameWrapper>
                <RightOutlined style={{ fontSize: '12px', height: '12px' }} />
            </MoreFilterOptionLabel>
        </ValueSelector>
    );
}
