import React from 'react';
import { Divider, Form, Select } from 'antd';
import styled from 'styled-components/macro';
import { Secret } from '../../../../../../types.generated';
import CreateSecretButton from './CreateSecretButton';
import { RecipeField } from '../common';

const StyledDivider = styled(Divider)`
    margin: 0;
`;

export const StyledFormItem = styled(Form.Item)<{ alignLeft?: boolean; removeMargin: boolean }>`
    margin-bottom: ${(props) => (props.removeMargin ? '0' : '16px')};

    ${(props) =>
        props.alignLeft &&
        `
        .ant-form-item {
            flex-direction: row;

        }

        .ant-form-item-label {
            padding: 0;
            margin-right: 10px;
        }
    `}
`;

interface SecretFieldProps {
    field: RecipeField;
    secrets: Secret[];
    removeMargin?: boolean;
    refetchSecrets: () => void;
}

function SecretField({ field, secrets, removeMargin, refetchSecrets }: SecretFieldProps) {
    return (
        <StyledFormItem name={field.name} label={field.label} tooltip={field.tooltip} removeMargin={!!removeMargin}>
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
        </StyledFormItem>
    );
}

export default SecretField;
