import { useApolloClient } from '@apollo/client';
import { AutoComplete, Input, radius, spacing } from '@components';
import { Divider } from 'antd';
import React, { ReactNode, useMemo } from 'react';
import styled from 'styled-components/macro';

import { clearSecretListCache } from '@app/ingestV2/secret/cacheUtils';
import { RecipeFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/RecipeFormItem';
import CreateSecretButton from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/SecretField/CreateSecretButton';
import { useSecrets } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/SecretField/useSecrets';
import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';
import { encodeSecret } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/utils';

const StyledDivider = styled(Divider)`
    margin: ${spacing.xsm} 0;
`;

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
                    href="https://docs.datahub.com/docs/ui-ingestion/#creating-a-secret"
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

export function SecretField({ field, updateFormValue }: CommonFieldProps) {
    const { secrets, refetchSecrets } = useSecrets();

    const options = useMemo(
        () => secrets.map((secret) => ({ value: encodeSecret(secret.name), label: secret.name })),
        [secrets],
    );
    const apolloClient = useApolloClient();

    return (
        <RecipeFormItem
            recipeField={field}
            tooltip={<SecretFieldTooltip tooltipLabel={field?.tooltip} />}
            showHelperText
        >
            <AutoComplete
                filterOption={(input, option) =>
                    !!option?.value?.toString()?.toLowerCase().includes(input.toLowerCase())
                }
                notFoundContent={<>No secrets found</>}
                options={options}
                clickOutsideWidth="100%"
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
                dropdownStyle={{
                    maxHeight: 1000,
                    overflowY: 'visible',
                    padding: spacing.xsm,
                    borderRadius: `${radius.md}`,
                }}
            >
                <Input placeholder={field.placeholder} />
            </AutoComplete>
        </RecipeFormItem>
    );
}
