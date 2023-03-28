import { CaretDownFilled } from '@ant-design/icons';
import { Button, Dropdown } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import FilterOption from './FilterOption';
import { FilterFields } from './types';
import { getNewFilters } from './utils';

const DropdownLabel = styled(Button)<{ isActive: boolean }>`
    font-size: 14px;
    font-weight: 700;
    margin-right: 12px;
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 8px;
    display: flex;
    align-items: center;
    box-shadow: none;

    ${(props) =>
        props.isActive &&
        `
        background-color: ${props.theme.styles['primary-color']};
        border: 1px solid ${props.theme.styles['primary-color']};
        color: white;
    `}
`;

const StyledButton = styled(Button)`
    width: 100%;
    text-align: center;
    background-color: ${(props) => props.theme.styles['primary-color']};
    color: white;
    border-radius: 0;
`;

const DropdownMenu = styled.div`
    background-color: white;
    border-radius: 5px;
    box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 9px 28px 8px rgba(0, 0, 0, 0.05);
    overflow: hidden;
    min-width: 200px;
`;

const ScrollableContent = styled.div`
    max-height: 312px;
    overflow: auto;
`;

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function SearchFilter({ filter, activeFilters, onChangeFilters }: Props) {
    const initialFilters = activeFilters.find((f) => f.field === filter.field)?.values;
    const [selectedFilterValues, setSelectedFilterValues] = useState<string[]>(initialFilters || []);
    const [isMenuOpen, setIsMenuOpen] = useState(false);

    const numActiveFilters = activeFilters.find((f) => f.field === filter.field)?.values?.length || 0;

    function handleMenuOpen(isOpen: boolean) {
        setIsMenuOpen(isOpen);
        // set filters to default every time you open or close the menu
        setSelectedFilterValues(initialFilters || []);
    }

    function updateFilters() {
        onChangeFilters(getNewFilters(filter.field, activeFilters, selectedFilterValues));
        setIsMenuOpen(false);
    }

    const filterOptions = filter.aggregations.map((agg) => {
        const filterFields: FilterFields = { field: filter.field, ...agg };
        return {
            key: agg.value,
            label: (
                <FilterOption
                    filterFields={filterFields}
                    selectedFilterValues={selectedFilterValues}
                    setSelectedFilterValues={setSelectedFilterValues}
                />
            ),
            style: { padding: 0 },
        };
    });

    return (
        <Dropdown
            trigger={['click']}
            menu={{ items: filterOptions }}
            open={isMenuOpen}
            overlayClassName="filter-dropdown"
            onOpenChange={(open) => handleMenuOpen(open)}
            dropdownRender={(menu) => (
                <DropdownMenu>
                    <ScrollableContent>
                        {React.cloneElement(menu as React.ReactElement, { style: { boxShadow: 'none' } })}
                    </ScrollableContent>
                    <StyledButton type="text" onClick={updateFilters}>
                        Update
                    </StyledButton>
                </DropdownMenu>
            )}
        >
            <DropdownLabel onClick={() => handleMenuOpen(!isMenuOpen)} isActive={!!numActiveFilters}>
                {filter.displayName} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                <CaretDownFilled style={{ fontSize: '12px', height: '12px' }} />
            </DropdownLabel>
        </Dropdown>
    );
}
