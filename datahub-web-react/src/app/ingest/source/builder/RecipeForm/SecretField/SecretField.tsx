import React from 'react';
import { Divider, Form, Select } from 'antd';
import styled from 'styled-components/macro';
import { RecipeField } from '../utils';
import { Secret } from '../../../../../../types.generated';
import CreateSecretButton from './CreateSecretButton';

const StyledDivider = styled(Divider)`
    margin: 0;
`;

interface SecretFieldProps {
    field: RecipeField;
    secrets: Secret[];
    refetchSecrets: () => void;
}

function SecretField({ field, secrets, refetchSecrets }: SecretFieldProps) {
    return (
        <Form.Item name={field.name} label={field.label} tooltip={field.tooltip}>
            <Select
                showSearch
                filterOption={(input, option) => !!option?.children.toLowerCase().includes(input.toLowerCase())}
                dropdownRender={(menu) => (
                    <>
                        {menu}
                        <StyledDivider />
                        <CreateSecretButton refetchSecrets={refetchSecrets} />
                    </>
                )}
            >
                {secrets.map((secret) => (
                    <Select.Option key={secret.urn} value={`\${${secret.name}}`}>
                        {secret.name}
                    </Select.Option>
                ))}
            </Select>
        </Form.Item>
    );
}

export default SecretField;
