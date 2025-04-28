import { Form } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { SimpleSelect } from '@src/alchemy-components';

const StyledFormItem = styled(Form.Item)<{ width: string }>`
    width: ${(props) => props.width};
    margin: 0;
`;

type Props = {
    selectedPath?: string;
    name: string;
    fields: { path: string; nativeType: string }[];
    onChange: (fieldValue: string) => void;
    width?: string;
    placeholder?: string;
    required?: boolean;
    disabled?: boolean;
    showSearch?: boolean;
};

export const AssertionDatasetFieldBuilder = ({
    selectedPath,
    name,
    fields,
    onChange,
    width = '100%',
    placeholder = 'Select a column...',
    required = true,
    disabled = false,
    showSearch = false,
}: Props) => {
    const columnOptions = fields.map((f) => ({
        label: f.path,
        value: f.path,
    }));
    return (
        <StyledFormItem
            initialValue={selectedPath}
            name={name}
            rules={[{ required, message: 'Required' }]}
            width={width}
        >
            <SimpleSelect
                placeholder={placeholder}
                options={columnOptions ?? []}
                onUpdate={(values) => onChange(values[0])}
                initialValues={selectedPath ? [selectedPath] : undefined}
                width="full"
                showSearch={showSearch}
                isDisabled={disabled}
            />
        </StyledFormItem>
    );
};
