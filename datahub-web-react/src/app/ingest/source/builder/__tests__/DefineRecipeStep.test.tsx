import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import defaultThemeConfig from '../../../../../conf/theme/theme_light.config.json';
import { DefineRecipeStep } from '../DefineRecipeStep';
import { SourceConfig } from '../types';

describe('DefineRecipeStep', () => {
    it('should render the RecipeBuilder if the type is in CONNECTORS_WITH_FORM', () => {
        const { getByText, queryByText } = render(
            <ThemeProvider theme={defaultThemeConfig}>
                <MockedProvider>
                    <DefineRecipeStep
                        state={{ type: 'snowflake' }}
                        updateState={() => {}}
                        goTo={() => {}}
                        submit={() => {}}
                        cancel={() => {}}
                        ingestionSources={[{ name: 'snowflake', displayName: 'Snowflake' } as SourceConfig]}
                    />
                </MockedProvider>
            </ThemeProvider>,
        );

        expect(getByText('Connection')).toBeInTheDocument();
        expect(queryByText('For more information about how to configure a recipe')).toBeNull();
    });

    it('should not render the RecipeBuilder if the type is not in CONNECTORS_WITH_FORM', () => {
        const { getByText, queryByText } = render(
            <ThemeProvider theme={defaultThemeConfig}>
                <MockedProvider>
                    <DefineRecipeStep
                        state={{ type: 'glue' }}
                        updateState={() => {}}
                        goTo={() => {}}
                        submit={() => {}}
                        cancel={() => {}}
                        ingestionSources={[{ name: 'glue', displayName: 'Glue' } as SourceConfig]}
                    />
                </MockedProvider>
            </ThemeProvider>,
        );

        expect(getByText('Configure Glue Recipe')).toBeInTheDocument();
        expect(queryByText('Connection')).toBeNull();
    });
});
