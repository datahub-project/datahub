import React from 'react';
import { Form, Select } from 'antd';
import { RecipeField } from './utils';
import { Secret } from '../../../../../types.generated';

interface SecretFieldProps {
    field: RecipeField;
    secrets: Secret[];
}

function SecretField({ field, secrets }: SecretFieldProps) {
    return (
        <Form.Item name={field.name} label={field.label} tooltip={field.tooltip}>
            <Select
                showSearch
                filterOption={(input, option) => !!option?.children.toLowerCase().includes(input.toLowerCase())}
            >
                {secrets.map((secret) => (
                    <Select.Option value={`\${${secret.name}}`}>{secret.name}</Select.Option>
                ))}
            </Select>
        </Form.Item>
    );
}

export default SecretField;
