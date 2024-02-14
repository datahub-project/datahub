import React, { useEffect } from 'react';
import { Form, InputNumber, Select } from 'antd';
import styled from 'styled-components';
import { Rule } from 'antd/lib/form';
import useFormInstance from 'antd/lib/form/hooks/useFormInstance';

const StyledFormItem = styled(Form.Item)`
    margin: 0;
`;

const StyledNumberInput = styled(InputNumber)`
    width: 300px;
`;

const StyledSelect = styled(Select)`
    width: 150px;
`;

type Props = {
    name: string;
    value?: number;
    onChange: (newValue: number) => void;
    placeholder: string;
    disabled?: boolean;
    select?: {
        value: string;
        options: { label: string; value: string }[];
        onChange: (newValue: string) => void;
    };
    customRules?: Rule[];
};

export const VolumeNumberInput = ({
    name,
    value,
    onChange,
    placeholder,
    disabled,
    select,
    customRules = [],
}: Props) => {
    const form = useFormInstance();
    useEffect(() => {
        form.setFieldValue(name, value);
    }, [form, name, value]);

    return (
        <StyledFormItem
            initialValue={value}
            name={name}
            rules={[{ required: true, message: 'Required' }, ...customRules]}
        >
            <StyledNumberInput
                placeholder={placeholder}
                min={0}
                addonAfter={
                    select ? (
                        <StyledSelect
                            value={select.value}
                            onChange={(newValue) => select.onChange(newValue as string)}
                            options={select.options}
                        />
                    ) : (
                        'rows'
                    )
                }
                onChange={(newValue) => onChange(newValue as number)}
                formatter={(newValue) => `${newValue}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}
                parser={(newValue) => (newValue ? newValue.replace(/(,*)/g, '') : '')}
                disabled={disabled}
            />
        </StyledFormItem>
    );
};
