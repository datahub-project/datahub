import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';

import SingleSelectInput from '@app/entity/shared/components/styled/StructuredProperty/SingleSelectInput';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { AllowedValue } from '@types';

const makeStringAllowedValue = (value: string, description?: string): AllowedValue => ({
    value: { __typename: 'StringValue', stringValue: value },
    description: description ?? null,
});

const SIX_ALLOWED_VALUES: AllowedValue[] = [
    makeStringAllowedValue('Apple', 'A fruit'),
    makeStringAllowedValue('Banana', 'Another fruit'),
    makeStringAllowedValue('Cherry'),
    makeStringAllowedValue('Date'),
    makeStringAllowedValue('Elderberry'),
    makeStringAllowedValue('Fig'),
];

function renderSelect(props?: Partial<React.ComponentProps<typeof SingleSelectInput>>) {
    return render(
        <CustomThemeProvider>
            <SingleSelectInput
                selectedValues={[]}
                allowedValues={SIX_ALLOWED_VALUES}
                selectSingleValue={vi.fn()}
                {...props}
            />
        </CustomThemeProvider>,
    );
}

describe('SingleSelectInput', () => {
    describe('Select dropdown (> 5 allowed values)', () => {
        it('renders a Select combobox instead of radio buttons', () => {
            renderSelect();
            expect(screen.getByRole('combobox')).toBeInTheDocument();
            expect(screen.queryByRole('radio')).not.toBeInTheDocument();
        });

        it('shows all options when dropdown is opened', async () => {
            renderSelect();
            const combobox = screen.getByRole('combobox');
            fireEvent.mouseDown(combobox);

            // Antd duplicates option text into accessible aria elements for visible options,
            // so some values appear twice in the DOM. Use getAllByText (length check) to handle
            // this gracefully regardless of how many aria elements antd creates.
            await waitFor(() => {
                expect(screen.queryAllByText('Apple').length).toBeGreaterThan(0);
                expect(screen.queryAllByText('Banana').length).toBeGreaterThan(0);
                expect(screen.queryAllByText('Fig').length).toBeGreaterThan(0);
            });
        });

        it('filters options when user types in the search input', async () => {
            renderSelect();
            const combobox = screen.getByRole('combobox');
            fireEvent.mouseDown(combobox);
            fireEvent.change(combobox, { target: { value: 'an' } });

            await waitFor(() => {
                expect(screen.getByRole('option', { name: 'Banana' })).toBeInTheDocument();
            });

            // Options not containing 'an' should be filtered out
            expect(screen.queryByRole('option', { name: 'Cherry' })).not.toBeInTheDocument();
            expect(screen.queryByRole('option', { name: 'Date' })).not.toBeInTheDocument();
            expect(screen.queryByRole('option', { name: 'Fig' })).not.toBeInTheDocument();
        });

        it('is case-insensitive when filtering', async () => {
            renderSelect();
            const combobox = screen.getByRole('combobox');
            fireEvent.mouseDown(combobox);
            fireEvent.change(combobox, { target: { value: 'APPLE' } });

            await waitFor(() => {
                expect(screen.getByRole('option', { name: 'Apple' })).toBeInTheDocument();
            });

            expect(screen.queryByRole('option', { name: 'Banana' })).not.toBeInTheDocument();
        });

        it('shows no options when filter matches nothing', async () => {
            renderSelect();
            const combobox = screen.getByRole('combobox');
            fireEvent.mouseDown(combobox);
            fireEvent.change(combobox, { target: { value: 'zzz' } });

            await waitFor(() => {
                expect(screen.queryByRole('option', { name: 'Apple' })).not.toBeInTheDocument();
                expect(screen.queryByRole('option', { name: 'Banana' })).not.toBeInTheDocument();
            });
        });

        it('calls selectSingleValue with the correct value when an option is clicked', async () => {
            const mockSelect = vi.fn();
            renderSelect({ selectSingleValue: mockSelect });
            const combobox = screen.getByRole('combobox');
            fireEvent.mouseDown(combobox);

            // Cherry is not the first option so it has no aria-activedescendant element.
            // Find it by visible text in the DropdownLabel, which appears exactly once.
            await waitFor(() => screen.getByText('Cherry'));
            fireEvent.click(screen.getByText('Cherry'));

            expect(mockSelect).toHaveBeenCalledWith('Cherry');
        });
    });

    describe('Radio group (<= 5 allowed values)', () => {
        const FIVE_VALUES: AllowedValue[] = SIX_ALLOWED_VALUES.slice(0, 5);

        it('renders radio buttons when there are 5 or fewer allowed values', () => {
            renderSelect({ allowedValues: FIVE_VALUES });
            expect(screen.getAllByRole('radio')).toHaveLength(5);
            expect(screen.queryByRole('combobox')).not.toBeInTheDocument();
        });
    });
});
