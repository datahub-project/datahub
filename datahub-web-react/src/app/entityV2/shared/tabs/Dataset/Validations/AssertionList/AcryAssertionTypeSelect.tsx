import { DownOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

const StyledSelect = styled(Select)<{ isSelected: boolean }>`
    min-width: 96px;
    &&& .ant-select-selector {
        font-size: 14px;
        font-weight: 400;
        border-radius: 8px;
        height: 36px;
        border: 1px solid ${REDESIGN_COLORS.SILVER_GREY};
        :hover {
            border: 1px solid ${REDESIGN_COLORS.BORDER_5};
        }
        box-shadow: none !important;
    }
    &&& .ant-select-selection-placeholder,
    &&& .ant-select-selection-item {
        color: ${(props) => (props.isSelected ? ANTD_GRAY[9] : ANTD_GRAY[7])};
        font-size: 14px !important;
        line-height: 34px !important;
    }
`;
const StyledSelectOption = styled(Select.Option)``;

const StyledSelectOptionLabel = styled.span`
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    font-size: 14px;
`;

type Option = {
    label: string;
    value: string;
};

type Props = {
    options: Option[];
    selectedValue: string | undefined;
    onSelect: (value: string) => void;
    placeholder: string;
};

export function AcryAssertionTypeSelect({ options, selectedValue, onSelect, placeholder }: Props) {
    const displayValue = selectedValue ? 'Group By (1)' : undefined;
    return (
        <StyledSelect
            isSelected={!!selectedValue}
            value={displayValue}
            onChange={(value) => {
                onSelect(value as string);
            }}
            suffixIcon={<DownOutlined />}
            placeholder={placeholder}
            allowClear
        >
            {options.map(({ label, value }) => (
                <StyledSelectOption key={value} value={value}>
                    <StyledSelectOptionLabel>{label}</StyledSelectOptionLabel>
                </StyledSelectOption>
            ))}
        </StyledSelect>
    );
}
