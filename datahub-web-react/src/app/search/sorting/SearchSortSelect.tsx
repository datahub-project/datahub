import Icon, { CaretDownFilled } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { DEFAULT_SORT_OPTION } from '@app/search/context/constants';
import useGetSortOptions from '@src/app/searchV2/sorting/useGetSortOptions';

import SortIcon from '@images/sort.svg?react';

const SelectWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    margin-right: 8px;
    margin-left: 15px;

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

type Props = {
    selectedSortOption: string | undefined;
    setSelectedSortOption: (option: string) => void;
};

export default function SearchSortSelect({ selectedSortOption, setSelectedSortOption }: Props) {
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
