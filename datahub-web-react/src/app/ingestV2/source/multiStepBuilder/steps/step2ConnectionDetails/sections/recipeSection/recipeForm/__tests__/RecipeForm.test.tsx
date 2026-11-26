import { LegacyForm as Form } from '@components';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React, { useState } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import YAML from 'yamljs';

import { QLIK_SENSE } from '@app/ingestV2/source/builder/constants';
import RecipeForm from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/RecipeForm';
import { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

vi.mock(
    '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/FormHeader',
    () => ({ FormHeader: () => null }),
);
vi.mock('@app/ingestV2/shared/hooks/useCapabilitySummary', () => ({
    useCapabilitySummary: () => ({ getConnectorsWithTestConnection: () => new Set<string>() }),
}));
vi.mock(
    '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/SettingsSection',
    () => ({ SettingsSection: () => null }),
);
vi.mock(
    '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/filtersSection/FiltersSection',
    () => ({ FiltersSection: () => null }),
);
// The secret picker loads secrets over GraphQL; a plain input keeps the field in the form.
vi.mock(
    '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/SecretField/SecretField',
    async () => {
        const { InputField } = await import(
            '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/InputField'
        );
        return { SecretField: InputField };
    },
);

const CLOUD_RECIPE = `source:
    type: qlik-sense
    config:
        tenant_hostname: example.us.qlikcloud.com
        api_key: api-key-value
        ingest_owner: true
`;

let stagedRecipe = CLOUD_RECIPE;

function Harness() {
    const [form] = Form.useForm();
    const [recipe, setRecipe] = useState(CLOUD_RECIPE);
    const setStagedRecipe = (value: string) => {
        stagedRecipe = value;
        setRecipe(value);
    };
    return (
        <CustomThemeProvider>
            <RecipeForm
                state={{ type: QLIK_SENSE } as MultiStepSourceBuilderState}
                displayRecipe={recipe}
                form={form}
                runFormValidation={() => undefined}
                setStagedRecipe={setStagedRecipe}
            />
        </CustomThemeProvider>
    );
}

async function selectDeployment(currentLabel: string, value: string) {
    // The first match is the selected value in the select; an open list repeats the label.
    fireEvent.click(screen.getAllByText(currentLabel)[0]);
    fireEvent.click(await screen.findByTestId(`option-${value}`));
}

describe('RecipeForm', () => {
    beforeEach(() => {
        mockVisibilityObserver();
        stagedRecipe = CLOUD_RECIPE;
    });

    it('keeps the Qlik Cloud settings after switching to on-prem and back', async () => {
        render(<Harness />);
        expect(await screen.findByDisplayValue('example.us.qlikcloud.com')).toBeInTheDocument();

        await selectDeployment('Qlik Cloud', 'on_prem');
        await waitFor(() => expect(YAML.parse(stagedRecipe).source.config.tenant_hostname).toBeUndefined());
        expect(YAML.parse(stagedRecipe).source.config.api_key).toBeUndefined();

        await selectDeployment('Qlik Sense Enterprise on Windows', 'cloud');
        await waitFor(() =>
            expect(YAML.parse(stagedRecipe).source.config).toMatchObject({
                tenant_hostname: 'example.us.qlikcloud.com',
                api_key: 'api-key-value',
                ingest_owner: true,
            }),
        );
    });
});
