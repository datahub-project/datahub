import { useApolloClient } from '@apollo/client';
import { AutoComplete, Divider, Form } from 'antd';
import React, { ReactNode } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { clearSecretListCache } from '@app/ingestV2/secret/cacheUtils';
import CreateSecretButton from '@app/ingestV2/source/builder/RecipeForm/SecretField/CreateSecretButton';
import { RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

import { Secret } from '@types';

const StyledDivider = styled(Divider)`
    margin: 0;
`;

export const StyledFormItem = styled(Form.Item)<{
    $alignLeft?: boolean;
    $removeMargin?: boolean;
    $isSecretField?: boolean;
}>`
    margin-bottom: ${(props) => (props.$removeMargin ? '0' : '16px')};

    ${(props) =>
        props.$alignLeft &&
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
        props.$isSecretField &&
        `
        .ant-form-item-label {
            &:after {
                /* untranslated-text -- CSS pseudo-element content evaluated outside React; t() cannot run here */
                content: 'Secret Field';
                color: ${props.theme.colors.textTertiary};
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
    updateFormValue: (field, value) => void;
}

function SecretFieldTooltip({ tooltipLabel }: { tooltipLabel?: string | ReactNode }) {
    const { t } = useTranslation('ingestion.sourceBuilder');
    return (
        <div>
            {tooltipLabel && (
                <>
                    {tooltipLabel}
                    <hr />
                </>
            )}
            <p>
                <Trans
                    t={t}
                    i18nKey="secret.docsHint.description"
                    components={{
                        anchor: (
                            <a
                                href="https://docs.datahub.com/docs/ui-ingestion/#creating-a-secret"
                                target="_blank"
                                rel="noreferrer"
                            >
                                {null}
                            </a>
                        ),
                    }}
                />
            </p>
        </div>
    );
}

const encodeSecret = (secretName: string) => {
    return `\${${secretName}}`;
};

function SecretField({ field, secrets, removeMargin, updateFormValue, refetchSecrets }: SecretFieldProps) {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const options = secrets.map((secret) => ({ value: encodeSecret(secret.name), label: secret.name }));
    const apolloClient = useApolloClient();

    return (
        <StyledFormItem
            required={field.required}
            name={field.name}
            label={field.label}
            rules={field.rules || undefined}
            tooltip={<SecretFieldTooltip tooltipLabel={field?.tooltip} />}
            $removeMargin={!!removeMargin}
            $isSecretField
        >
            <AutoComplete
                placeholder={field.placeholder}
                filterOption={(input, option) => !!option?.value?.toLowerCase().includes(input.toLowerCase())}
                notFoundContent={t('secret.notFound')}
                options={options}
                dropdownRender={(menu) => {
                    return (
                        <>
                            {menu}
                            <StyledDivider />
                            <CreateSecretButton
                                onSubmit={(state) => {
                                    updateFormValue(field.name, encodeSecret(state.name as string));
                                    setTimeout(() => clearSecretListCache(apolloClient), 3000);
                                }}
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
