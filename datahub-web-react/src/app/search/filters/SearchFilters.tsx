import { CaretDownFilled } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';

const SearchFiltersWrapper = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: 8px 24px;
`;

const FilterDropdownsWrapper = styled.div`
    display: flex;
`;

const DropdownLabel = styled.div`
    font-size: 14px;
    font-weight: 700;
`;

interface Props {
    availableFilters: FacetMetadata[] | null;
    activeFilters: FacetFilterInput[];
}

const items = [
    {
        key: '1',
        label: (
            <a target="_blank" rel="noopener noreferrer" href="https://www.antgroup.com">
                1st menu item
            </a>
        ),
    },
    {
        key: '2',
        label: (
            <a target="_blank" rel="noopener noreferrer" href="https://www.aliyun.com">
                2nd menu item (disabled)
            </a>
        ),
        disabled: true,
    },
    {
        key: '3',
        label: (
            <a target="_blank" rel="noopener noreferrer" href="https://www.luohanacademy.com">
                3rd menu item (disabled)
            </a>
        ),
        disabled: true,
    },
];

export default function SearchFilters({ availableFilters, activeFilters }: Props) {
    console.log('availableFilters', availableFilters);
    console.log('activeFilters', activeFilters);
    console.log('activeFilters', activeFilters);

    return (
        <SearchFiltersWrapper>
            <FilterDropdownsWrapper>
                {availableFilters?.map((filter) => {
                    return (
                        <Dropdown trigger={['click']} menu={{ items }}>
                            <DropdownLabel>
                                {filter.displayName} <CaretDownFilled />
                            </DropdownLabel>
                        </Dropdown>
                    );
                })}
            </FilterDropdownsWrapper>
        </SearchFiltersWrapper>
    );
}
