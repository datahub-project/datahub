import { CaretDownFilled } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { DEFAULT_SORT_OPTION } from '@app/searchV2/context/constants';
import useGetSortOptions from '@app/searchV2/sorting/useGetSortOptions';

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
        color: ${(props) => props.theme.colors.textTertiary} !important;
        font-weight: 700;
    }

    .ant-select-selection-placeholder {
        color: ${(props) => props.theme.colors.textTertiary};
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
