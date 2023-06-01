import { PlusOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import MoreFilterOption from './MoreFilterOption';
import { getNumActiveFiltersForGroupOfFilters } from './utils';
import { DropdownLabel } from './SearchFilterView';

const StyledPlus = styled(PlusOutlined)`
    svg {
        height: 12px;
        width: 12px;
    }
`;

const DropdownMenu = styled.div<{ padding?: string }>`
    background-color: white;
    border-radius: 5px;
    box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 9px 28px 8px rgba(0, 0, 0, 0.05);
    overflow: hidden;
    min-width: 200px;
    max-width: 240px;

    ${(props) => props.padding !== undefined && `padding: ${props.padding};`}
`;

interface Props {
    filters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function MoreFilters({ filters, activeFilters, onChangeFilters }: Props) {
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const numActiveFilters = getNumActiveFiltersForGroupOfFilters(activeFilters, filters);

    function updateFiltersAndClose(newFilters: FacetFilterInput[]) {
        onChangeFilters(newFilters);
        setIsMenuOpen(false);
    }

    return (
        <Dropdown
            trigger={['click']}
            dropdownRender={() => (
                <DropdownMenu padding="4px 0px">
                    {filters.map((filter) => (
                        <MoreFilterOption
                            filter={filter}
                            activeFilters={activeFilters}
                            onChangeFilters={updateFiltersAndClose}
                        />
                    ))}
                </DropdownMenu>
            )}
            open={isMenuOpen}
            onOpenChange={(open) => setIsMenuOpen(open)}
        >
            <DropdownLabel isActive={!!numActiveFilters}>
                <StyledPlus />
                More Filters {numActiveFilters ? `(${numActiveFilters}) ` : ''}
            </DropdownLabel>
        </Dropdown>
    );
}
