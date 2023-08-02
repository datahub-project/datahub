import React from 'react';
import styled from 'styled-components';
import { Form, Select } from 'antd';
import { FreshnessFieldSpec } from '../../../../../../../../../../types.generated';

const StyledFormItem = styled(Form.Item)`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;
const ColumnSelect = styled(Select)`
    width: 340px;
`;

type Props = {
    fields: Partial<FreshnessFieldSpec>[];
    value?: FreshnessFieldSpec | null;
    onChange: (newField: Partial<FreshnessFieldSpec>) => void;
};

/**
 * Builder used to construct an Freshness based on a field / column value.
 */
export const FieldValueSourceBuilder = ({ fields, value, onChange }: Props) => {
    const updateFreshnessFieldSpec = (newPath: any) => {
        const spec = fields.filter((field) => field.path === newPath)[0];
        onChange(spec);
    };

    return (
        <StyledFormItem name="column" rules={[{ required: true, message: 'Please select a column' }]}>
            <ColumnSelect placeholder="Select a column..." value={value?.path} onChange={updateFreshnessFieldSpec}>
                {fields.map((field) => (
                    <Select.Option value={field.path}>{field.path}</Select.Option>
                ))}
            </ColumnSelect>
        </StyledFormItem>
    );
};
