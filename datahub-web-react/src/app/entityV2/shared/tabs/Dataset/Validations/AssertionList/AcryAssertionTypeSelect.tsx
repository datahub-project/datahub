import { CaretDownOutlined } from '@ant-design/icons';
import { ANTD_GRAY_V2, REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

const StyledSelect = styled(Select)`
    min-width: 96px;
    &&& .ant-select-selector {
        font-size: 12px;
        font-weight: 500;
        line-height: 24px;
        color: ${ANTD_GRAY_V2[8]};
        border-radius: 8px;
        :hover {
            border: 1px solid ${REDESIGN_COLORS.TITLE_PURPLE};
            color: ${ANTD_GRAY_V2[8]};
        }
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
