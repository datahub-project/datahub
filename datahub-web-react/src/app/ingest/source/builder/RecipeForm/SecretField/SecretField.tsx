import React, { ReactNode } from 'react';
import { AutoComplete, Divider, Form, FormInstance } from 'antd';
import styled from 'styled-components/macro';
import { Secret } from '../../../../../../types.generated';
import CreateSecretButton from './CreateSecretButton';
import { RecipeField } from '../common';
import { ANTD_GRAY } from '../../../../../entity/shared/constants';

const StyledDivider = styled(Divider)`
    margin: 0;
`;

export const StyledFormItem = styled(Form.Item)<{
    alignLeft?: boolean;
    removeMargin?: boolean;
    isSecretField?: boolean;
}>`
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

    ${(props) =>
        props.isSecretField &&
        `
        .ant-form-item-label {
            &:after {
                content: 'Secret Field';
                color: ${ANTD_GRAY[7]};
                font-style: italic;
                font-weight: 100;
                margin-left: 5px;
                font-size: 10px;
            }
        }
    `}
`;

interface SecretFieldProps {
    field: RecipeField;
    secrets: Secret[];
    removeMargin?: boolean;
    refetchSecrets: () => void;
    form: FormInstance<any>;
}

function SecretFieldTooltip({ tooltipLabel }: { tooltipLabel?: string | ReactNode }) {
    return (
        <div>
            {tooltipLabel && (
                <>
                    {tooltipLabel}
                    <hr />
                </>
            )}
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
                .
            </p>
        </div>
    );
}

const encodeSecret = (secretName: string) => {
    return `\${${secretName}}`;
};

function SecretField({ field, secrets, removeMargin, form, refetchSecrets }: SecretFieldProps) {
    const options = secrets.map((secret) => ({ value: encodeSecret(secret.name), label: secret.name }));

    return (
        <StyledFormItem
            required={field.required}
            name={field.name}
            label={field.label}
            rules={field.rules || undefined}
            tooltip={<SecretFieldTooltip tooltipLabel={field?.tooltip} />}
            removeMargin={!!removeMargin}
            isSecretField
        >
            <AutoComplete
                placeholder={field.placeholder}
                filterOption={(input, option) => !!option?.value.toLowerCase().includes(input.toLowerCase())}
                notFoundContent={<>No secrets found</>}
                options={options}
                dropdownRender={(menu) => {
                    return (
                        <>
                            {menu}
                            <StyledDivider />
                            <CreateSecretButton
                                onSubmit={(state) =>
                                    form.setFields([{ name: field.name, value: encodeSecret(state.name as string) }])
                                }
                                refetchSecrets={refetchSecrets}
                            />
                        </>
                    );
                }}
            />
        </StyledFormItem>
    );
}

export default SecretField;
