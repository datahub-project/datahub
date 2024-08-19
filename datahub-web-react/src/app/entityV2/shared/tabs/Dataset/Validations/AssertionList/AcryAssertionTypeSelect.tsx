import { CaretDownOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

const StyledSelect = styled(Select)`
    min-width: 250px;
    max-width: 250px;
    &&& .ant-select-selector {
        font-size: 14px;
        font-weight: 500;
        line-height: 24px;
        color: #595959;
        border-radius: 8px;
    }
`;

type Props = {
    options: Array<{ label: string; value: string }>;
    selectedValue: string;
    onSelect: (value: string | null) => void;
    placeholder: string;
};

export default function AcryAssertionTypeSelect({ options, selectedValue, onSelect, placeholder }: Props) {
    return (
        <StyledSelect
            value={selectedValue || undefined}
            onChange={(value) => {
                onSelect(value as string);
            }}
            suffixIcon={<CaretDownOutlined />}
            placeholder={placeholder}
            allowClear
        >
            {options.map((option) => (
                <StyledSelect.Option key={option.value} value={option.value}>
                    {option.label}
                </StyledSelect.Option>
            ))}
        </StyledSelect>
    );
}