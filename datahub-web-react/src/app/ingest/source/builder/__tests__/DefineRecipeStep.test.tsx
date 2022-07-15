import { render } from '@testing-library/react';
import React from 'react';
import { DefineRecipeStep } from '../DefineRecipeStep';

describe('DefineRecipeStep', () => {
    it('should render the RecipeBuilder if the type is in CONNECTORS_WITH_FORM', () => {
        const { getByText, queryByText } = render(
            <DefineRecipeStep
                state={{ type: 'snowflake' }}
                updateState={() => {}}
                goTo={() => {}}
                submit={() => {}}
                cancel={() => {}}
            />,
        );

        expect(getByText('Connection')).toBeInTheDocument();
        expect(queryByText('For more information about how to configure a recipe')).toBeNull();
    });

    it('should not render the RecipeBuilder if the type is not in CONNECTORS_WITH_FORM', () => {
        const { getByText, queryByText } = render(
            <DefineRecipeStep
                state={{ type: 'postgres' }}
                updateState={() => {}}
                goTo={() => {}}
                submit={() => {}}
                cancel={() => {}}
            />,
        );

        expect(getByText('Configure Postgres Recipe')).toBeInTheDocument();
        expect(queryByText('Connection')).toBeNull();
    });
});
