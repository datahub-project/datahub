import { DownOutlined } from '@ant-design/icons';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

const StyledSelect = styled(Select)`
    min-width: 96px;
    &&& .ant-select-selector {
        font-size: 14px;
        font-weight: 500;
        line-height: 24px;
        border-radius: 8px;
        :hover {
            border: 2px solid ${REDESIGN_COLORS.BORDER_5};
        }
        box-shadow: none !important;
    }
    &&& .ant-select-selection-placeholder {
        color: ${REDESIGN_COLORS.TEXT_HEADING} !important;
        font-size: 14px !important;
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
    selectedValue: string;
    onSelect: (value: string) => void;
    placeholder: string;
};

export function AcryAssertionTypeSelect({ options, selectedValue, onSelect, placeholder }: Props) {
    const displayValue = selectedValue ? 'Group By(1)' : undefined;
    return (
        <StyledSelect
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
