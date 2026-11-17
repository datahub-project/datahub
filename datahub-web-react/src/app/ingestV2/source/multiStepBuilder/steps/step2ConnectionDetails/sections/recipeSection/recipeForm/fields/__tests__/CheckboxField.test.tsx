import { render, screen } from '@testing-library/react';
import { Form } from 'antd';
import React from 'react';
import { describe, expect, it } from 'vitest';

import '@app/context/import/__tests__/testSetup';
import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import { CheckboxField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/CheckboxField';
import CustomThemeProvider from '@src/CustomThemeProvider';

const baseField: RecipeField = {
    name: 'example_checkbox',
    label: 'Example checkbox',
    tooltip: 'Example checkbox tooltip',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.example',
    rules: null,
};

function renderCheckboxField(field: RecipeField) {
    return render(
        <CustomThemeProvider>
            <Form>
                <CheckboxField field={field} updateFormValue={() => undefined} />
            </Form>
        </CustomThemeProvider>,
    );
}

describe('CheckboxField', () => {
    it('renders short helper text inline when helper is defined', () => {
        renderCheckboxField({
            ...baseField,
            helper: 'Short helper text',
            tooltip: 'Long tooltip text that should not appear inline',
        });

        expect(screen.getByText('Short helper text')).toBeInTheDocument();
        expect(screen.queryByText('Long tooltip text that should not appear inline')).not.toBeInTheDocument();
    });

    it('does not render tooltip text inline when only tooltip is defined', () => {
        renderCheckboxField({
            ...baseField,
            tooltip: 'Tooltip-only details',
        });

        expect(screen.getByText('Example checkbox')).toBeInTheDocument();
        expect(screen.queryByText('Tooltip-only details')).not.toBeInTheDocument();
    });
});
