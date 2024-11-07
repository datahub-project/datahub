import Icon, { CaretDownFilled } from '@ant-design/icons';
import { Select } from 'antd';
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';
import useGetSortOptions from '@src/app/searchV2/sorting/useGetSortOptions';
import SortIcon from '../../../images/sort.svg?react';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { DEFAULT_SORT_OPTION } from '../context/constants';
import { useSearchContext } from '../context/SearchContext';

const SelectWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    margin-right: 8px;

    .ant-select-selection-item {
        // !important is necessary because updating Select styles for antd is impossible
        color: ${ANTD_GRAY[8]} !important;
        font-weight: 700;
    }

    .ant-select-selection-placeholder {
        color: ${ANTD_GRAY[8]};
        font-weight: 700;
    }
`;

const StyledIcon = styled(Icon)`
    color: ${ANTD_GRAY[8]};
    font-size: 16px;
    margin-right: -8px;
`;

export default function SearchSortSelect() {
    const { selectedSortOption, setSelectedSortOption } = useSearchContext();
    const sortOptions = useGetSortOptions();

    const options = Object.entries(sortOptions).map(([value, option]) => ({ value, label: option.label }));

    return (
        <Tooltip title="Sort search results" showArrow={false} placement="left">
            <SelectWrapper>
                <StyledIcon component={SortIcon} />
                <Select
                    placeholder="Sort"
                    value={selectedSortOption === DEFAULT_SORT_OPTION ? null : selectedSortOption}
                    options={options}
                    bordered={false}
                    onChange={(sortOption) => setSelectedSortOption(sortOption)}
                    dropdownStyle={{ minWidth: 'max-content' }}
                    placement="bottomRight"
                    suffixIcon={<CaretDownFilled />}
                />
            </SelectWrapper>
        </Tooltip>
    );
}
