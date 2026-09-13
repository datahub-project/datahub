import { render } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { vi } from 'vitest';

import RecipeBuilder from '@app/ingestV2/source/builder/RecipeBuilder';
import { SourceBuilderState, SourceConfig } from '@app/ingestV2/source/builder/types';
import themeV2 from '@conf/theme/themeV2';

// RecipeForm pulls in GraphQL + a large form tree that is irrelevant to the
// banner-integration logic under test. Stub it so the test isolates the
// parsedRecipe memo + Snowflake conditional in RecipeBuilder itself.
vi.mock('@app/ingestV2/source/builder/RecipeForm/RecipeForm', () => ({
    __esModule: true,
    default: () => <div data-testid="recipe-form" />,
}));

const sourceConfig = {
    urn: 'urn:li:dataPlatform:snowflake',
    name: 'snowflake',
    displayName: 'Snowflake',
    docsUrl: '',
    recipe: '',
} as SourceConfig;

function renderBuilder(state: SourceBuilderState, displayRecipe: string) {
    return render(
        <ThemeProvider theme={themeV2}>
            <RecipeBuilder
                state={state}
                isEditing={false}
                displayRecipe={displayRecipe}
                sourceConfigs={sourceConfig}
                setStagedRecipe={() => {}}
                onClickNext={() => {}}
                goToPrevious={() => {}}
            />
        </ThemeProvider>,
    );
}

describe('RecipeBuilder Snowflake password-auth deprecation banner', () => {
    it('renders the deprecation banner for a Snowflake password-auth recipe', () => {
        const recipe = `
source:
  config:
    authentication_type: DEFAULT_AUTHENTICATOR
    password: secret
`;
        const { getByText, container } = renderBuilder({ type: 'snowflake' }, recipe);

        expect(getByText(/Snowflake is deprecating username \+ password authentication/)).toBeInTheDocument();
        expect(container.querySelector('a[href$="migrate-to-key-pair-auth"]')?.getAttribute('href')).toBe(
            'https://docs.datahub.com/docs/quick-ingestion-guides/snowflake/migrate-to-key-pair-auth',
        );
    });

    it('does not render the banner for a Snowflake key-pair recipe', () => {
        const recipe = `
source:
  config:
    authentication_type: KEY_PAIR_AUTHENTICATOR
    private_key: -----BEGIN PRIVATE KEY-----
`;
        const { queryByText, container } = renderBuilder({ type: 'snowflake' }, recipe);

        expect(queryByText(/Snowflake is deprecating username \+ password authentication/)).toBeNull();
        // Banner Alert is the only element rendered conditionally; the form stub remains.
        expect(container.querySelector('[data-testid="snowflake-password-auth-deprecation-warning"]')).toBeNull();
    });

    it('does not render the banner for a non-Snowflake recipe', () => {
        // MySQL recipe that happens to carry a password field — must not trigger the
        // Snowflake-only banner, and the memo must skip YAML parsing entirely.
        const recipe = `
source:
  config:
    password: secret
`;
        const { queryByText, container } = renderBuilder({ type: 'mysql' }, recipe);

        expect(queryByText(/Snowflake is deprecating username \+ password authentication/)).toBeNull();
        expect(container.querySelector('[data-testid="snowflake-password-auth-deprecation-warning"]')).toBeNull();
    });
});
