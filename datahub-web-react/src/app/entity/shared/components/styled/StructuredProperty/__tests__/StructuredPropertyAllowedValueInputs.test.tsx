import { fireEvent, render, screen, within } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import MultiSelectInput from '@app/entity/shared/components/styled/StructuredProperty/MultiSelectInput';
import NumberInput from '@app/entity/shared/components/styled/StructuredProperty/NumberInput';
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
    it('renders a null single-cardinality number as an empty input', () => {
        render(wrapper(<NumberInput selectedValues={[null]} updateSelectedValues={vi.fn()} />));

        expect(screen.getByRole('spinbutton')).toHaveValue(null);
    });

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

    it('gives each single-select an accessible radio group with unique input ids', () => {
        render(
            wrapper(
                <>
                    <SingleSelectInput
                        allowedValues={DEFINITION_ORDER}
                        selectedValues={[]}
                        selectSingleValue={vi.fn()}
                    />
                    <SingleSelectInput
                        allowedValues={DEFINITION_ORDER}
                        selectedValues={[]}
                        selectSingleValue={vi.fn()}
                    />
                </>,
            ),
        );

        const groups = screen.getAllByRole('radiogroup');
        const firstGroupRadios = within(groups[0]).getAllByRole('radio');
        const secondGroupRadios = within(groups[1]).getAllByRole('radio');

        expect(new Set(firstGroupRadios.map((radio) => radio.getAttribute('name'))).size).toBe(1);
        expect(firstGroupRadios[0].getAttribute('name')).not.toBe(secondGroupRadios[0].getAttribute('name'));
        expect(new Set([...firstGroupRadios, ...secondGroupRadios].map((radio) => radio.id)).size).toBe(6);
    });

    it('does not offer an unsupported clear action for a single-select dropdown', () => {
        const allowedValues = [
            ...DEFINITION_ORDER,
            makeAllowedValue('Value four'),
            makeAllowedValue('Value five'),
            makeAllowedValue('Value six'),
        ];
        render(
            wrapper(
                <SingleSelectInput
                    allowedValues={allowedValues}
                    selectedValues={['Value one']}
                    selectSingleValue={vi.fn()}
                />,
            ),
        );

        expect(screen.queryByTestId('button-clear')).not.toBeInTheDocument();
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
