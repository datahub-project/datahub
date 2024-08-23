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
            {options.map(({ label, value }) => (
                <Select.Option key={value} value={value}>
                    {label}
                </Select.Option>
            ))}
        </StyledSelect>
    );
}
