import Icon, { CaretDownFilled } from '@ant-design/icons';
import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ReactComponent as SortIcon } from '../../../images/sort.svg';
import { DEFAULT_SORT_OPTION, SORT_OPTIONS } from '../context/constants';
import { useSearchContext } from '../context/SearchContext';

const SelectWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    margin-right: 8px;

    .ant-select-selection-item {
        // !important is necessary because updating Select styles for antd is impossible
        color: ${(props) => props.theme.styles['primary-color']} !important;
        font-weight: 700;
    }

    svg {
        color: ${(props) => props.theme.styles['primary-color']};
    }
`;

const StyledIcon = styled(Icon)`
    color: ${(props) => props.theme.styles['primary-color']};
    font-size: 16px;
    margin-right: -6px;
`;

export default function SearchSortSelect() {
    const { selectedSortOption, setSelectedSortOption } = useSearchContext();

    const options = Object.entries(SORT_OPTIONS).map(([value, option]) => ({ value, label: option.label }));

    return (
        <SelectWrapper>
            <StyledIcon component={SortIcon} />
            <Select
                value={selectedSortOption}
                defaultValue={DEFAULT_SORT_OPTION}
                options={options}
                bordered={false}
                onChange={(sortOption) => setSelectedSortOption(sortOption)}
                dropdownStyle={{ minWidth: 'max-content' }}
                placement="bottomRight"
                suffixIcon={<CaretDownFilled />}
            />
        </SelectWrapper>
    );
}
