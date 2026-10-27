import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import { Form } from 'antd';
import React from 'react';
import { MockedFunction, describe, expect, it, vi } from 'vitest';

import '@app/context/import/__tests__/testSetup';
import { useUserContext } from '@app/context/useUserContext';
import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import { SecretField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/SecretField/SecretField';
import CustomThemeProvider from '@src/CustomThemeProvider';

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

// Render the dropdown contents directly: opening the real dropdown in jsdom crashes inside rc-select.
vi.mock('@components', async (importOriginal) => ({
    ...(await importOriginal<object>()),
    AutoComplete: ({ dropdownRender }: { dropdownRender: (menu: React.ReactNode) => React.ReactNode }) => (
        <>{dropdownRender(null)}</>
    ),
}));

vi.mock(
    '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/SecretField/useSecrets',
    () => ({
        useSecrets: () => ({ secrets: [], refetchSecrets: vi.fn() }),
    }),
);

const field: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'Password tooltip',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    rules: null,
};

function renderSecretField(manageSecrets: boolean) {
    (useUserContext as MockedFunction<typeof useUserContext>).mockReturnValue({
        platformPrivileges: { manageSecrets },
    } as ReturnType<typeof useUserContext>);
    render(
        <MockedProvider>
            <CustomThemeProvider>
                <Form>
                    <SecretField field={field} updateFormValue={() => undefined} />
                </Form>
            </CustomThemeProvider>
        </MockedProvider>,
    );
}

describe('SecretField', () => {
    it('offers Create Secret to users who can manage secrets', () => {
        renderSecretField(true);
        expect(screen.getByText('Create Secret')).toBeInTheDocument();
    });

    it('hides Create Secret from users who cannot manage secrets', () => {
        renderSecretField(false);
        expect(screen.queryByText('Create Secret')).not.toBeInTheDocument();
    });
});
