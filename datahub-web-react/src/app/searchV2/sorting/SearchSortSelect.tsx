import React from 'react';
import { CaretDownFilled } from '@ant-design/icons';
import { Select } from 'antd';
import { Tooltip } from '@components';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { DEFAULT_SORT_OPTION } from '../context/constants';
import useGetSortOptions from './useGetSortOptions';

const SelectWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    margin-right: 0px;
    && {
        padding: 0px;
        margin: 0px;
    }
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
                <Select
                    placeholder="Sort by"
                    value={selectedSortOption === DEFAULT_SORT_OPTION ? null : selectedSortOption}
                    options={options}
                    bordered={false}
                    onChange={(option) => setSelectedSortOption(option)}
                    dropdownStyle={{ minWidth: 'max-content' }}
                    placement="bottomRight"
                    suffixIcon={<CaretDownFilled />}
                />
            </SelectWrapper>
        </Tooltip>
    );
}
