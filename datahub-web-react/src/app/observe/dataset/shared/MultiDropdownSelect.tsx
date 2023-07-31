import React from 'react';
import { Select } from 'antd';

const DROPDOWN_STYLE = { minWidth: 48, textAlign: 'center' };

type Props = {
    value: string[] | undefined;
    options: { name: string; value: string }[];
    onChange: (values: string[]) => void;
};

export const MultiDropdownSelect = ({ value, options, onChange }: Props) => {
    return (
        <Select
            style={DROPDOWN_STYLE as any}
            dropdownStyle={{ minWidth: 120, maxWidth: 200 }}
            placeholder="any"
            value={value}
            mode="multiple"
            options={options.map((option) => ({ value: option.value, label: option.name }))}
            onChange={onChange}
            showSearch={false}
        />
    );
};
