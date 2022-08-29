import React from 'react';
import { Divider, Form, Select, Tooltip } from 'antd';
import styled from 'styled-components/macro';
import { Secret } from '../../../../../../types.generated';
import CreateSecretButton from './CreateSecretButton';
import { RecipeField } from '../common';
import { ANTD_GRAY } from '../../../../../entity/shared/constants';

const StyledDivider = styled(Divider)`
    margin: 0;
`;

export const SecretFieldLabel = styled.span`
    color: ${ANTD_GRAY[7]};
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

function getTooltip() {
    return (
        <div>
            <p>
                This field requires you to use a DataHub Secret. For more information on Secrets in DataHub, please
                review{' '}
                <a
                    href="https://datahubproject.io/docs/ui-ingestion/#creating-a-secret"
                    target="_blank"
                    rel="noreferrer"
                >
                    the docs
                </a>
            </p>
        </div>
    );
}

function SecretField({ field, secrets, removeMargin, refetchSecrets }: SecretFieldProps) {
    return (
        <StyledFormItem name={field.name} label={field.label} tooltip={field.tooltip} removeMargin={!!removeMargin}>
            <Tooltip title={getTooltip}>
                <SecretFieldLabel>Secret Field</SecretFieldLabel>
            </Tooltip>
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
