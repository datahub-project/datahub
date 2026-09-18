import { fireEvent, render, screen, within } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import MultiSelectInput from '@app/entity/shared/components/styled/StructuredProperty/MultiSelectInput';
import SingleSelectInput from '@app/entity/shared/components/styled/StructuredProperty/SingleSelectInput';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

import { AllowedValue } from '@types';

function makeAllowedValue(value: string, description?: string): AllowedValue {
    return {
        value: { __typename: 'StringValue', stringValue: value },
        description,
    };
}

const DEFINITION_ORDER = [
    makeAllowedValue('Value one'),
    makeAllowedValue('Banana', 'Second in the definition'),
    makeAllowedValue('Apple'),
];

const wrapper = (children: React.ReactNode) => <CustomThemeProvider>{children}</CustomThemeProvider>;

beforeEach(mockVisibilityObserver);

describe('structured-property allowed-value inputs', () => {
    it('renders single-select choices in definition order and returns the original value', () => {
        const selectSingleValue = vi.fn();
        render(
            wrapper(
                <SingleSelectInput
                    allowedValues={DEFINITION_ORDER}
                    selectedValues={[]}
                    selectSingleValue={selectSingleValue}
                />,
            ),
        );

        const choices = screen.getAllByRole('radio');
        expect(choices.map((choice) => choice.getAttribute('aria-label'))).toEqual(['Value one', 'Banana', 'Apple']);

        fireEvent.click(choices[1]);
        expect(selectSingleValue).toHaveBeenCalledWith('Banana');
    });

    it('renders multi-select choices in definition order and toggles the original value', () => {
        const toggleSelectedValue = vi.fn();
        render(
            wrapper(
                <MultiSelectInput
                    allowedValues={DEFINITION_ORDER}
                    selectedValues={[]}
                    toggleSelectedValue={toggleSelectedValue}
                    updateSelectedValues={vi.fn()}
                />,
            ),
        );

        const choices = screen.getAllByRole('checkbox');
        expect(choices.map((choice) => choice.getAttribute('value'))).toEqual(['Value one', 'Banana', 'Apple']);

        fireEvent.click(choices[1]);
        expect(toggleSelectedValue).toHaveBeenCalledWith('Banana');
    });

    it('keeps definition order in a multi-select dropdown even when a later value is selected', () => {
        const allowedValues = [
            makeAllowedValue('Value one'),
            makeAllowedValue('Value two'),
            makeAllowedValue('Value three'),
            makeAllowedValue('Banana'),
            makeAllowedValue('Apple'),
            makeAllowedValue('Pear'),
        ];
        render(
            wrapper(
                <MultiSelectInput
                    allowedValues={allowedValues}
                    selectedValues={['Apple']}
                    toggleSelectedValue={vi.fn()}
                    updateSelectedValues={vi.fn()}
                />,
            ),
        );

        fireEvent.click(screen.getByTestId('structured-property-multi-select-base'));

        const dropdown = screen.getByTestId('structured-property-multi-select-dropdown');
        const labels = within(dropdown)
            .getAllByText(/^(Value one|Value two|Value three|Banana|Apple|Pear)$/)
            .map((label) => label.textContent);
        expect(labels).toEqual(['Value one', 'Value two', 'Value three', 'Banana', 'Apple', 'Pear']);
    });
});
