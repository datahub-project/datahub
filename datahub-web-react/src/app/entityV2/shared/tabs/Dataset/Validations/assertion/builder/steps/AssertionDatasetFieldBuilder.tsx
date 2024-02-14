import React from 'react';
import Typography from 'antd/lib/typography';
import styled from 'styled-components';
import { Divider, Form, Select } from 'antd';

const StyledFormItem = styled(Form.Item)<{ width: string }>`
    width: ${(props) => props.width};
    margin: 0;
`;

type Props = {
    name: string;
    fields: { path: string; nativeType: string }[];
    onChange: (fieldValue: string) => void;
    width?: string;
    placeholder?: string;
    required?: boolean;
    disabled?: boolean;
};

export const AssertionDatasetFieldBuilder = ({
    name,
    fields,
    onChange,
    width = '100%',
    placeholder = 'Select a column...',
    required = true,
    disabled = false,
}: Props) => (
    <StyledFormItem name={name} rules={[{ required, message: 'Required' }]} width={width}>
        <Select
            placeholder={placeholder}
            onChange={(newFieldPath) => onChange(newFieldPath as string)}
            optionLabelProp="label"
            disabled={disabled}
        >
            {fields.map((field) => (
                <Select.Option key={field.path} value={field.path}>
                    <Typography.Text>{field.path}</Typography.Text>
                    <Divider type="vertical" />
                    <Typography.Text type="secondary">{field.nativeType}</Typography.Text>
                </Select.Option>
            ))}
        </Select>
    </StyledFormItem>
);
