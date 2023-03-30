import { PlusOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import MoreFilterOption from './MoreFilterOption';
import { DropdownLabel, DropdownMenu } from './SearchFilter';
import { getNumActiveFiltersForGroupOfFilters } from './utils';

const StyledPlus = styled(PlusOutlined)`
    svg {
        height: 12px;
        width: 12px;
    }
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
