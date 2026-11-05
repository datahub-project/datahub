import { fireEvent, render, screen } from '@testing-library/react';
import React, { useState } from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { Radio, RadioGroup } from '@components/components/Radio/Radio';

import themeV2 from '@conf/theme/themeV2';

const RADIOS = [{ label: 'First' }, { label: 'Second' }, { label: 'Third' }];

function renderWithTheme(ui: React.ReactElement) {
    return render(<ThemeProvider theme={themeV2}>{ui}</ThemeProvider>);
}

describe('RadioGroup', () => {
    it('keeps only one radio checked when a second radio is clicked', () => {
        renderWithTheme(<RadioGroup radios={RADIOS} />);

        fireEvent.click(screen.getByLabelText('First'));
        fireEvent.click(screen.getByLabelText('Second'));

        expect(screen.getByLabelText('First')).not.toBeChecked();
        expect(screen.getByLabelText('Second')).toBeChecked();
        expect(screen.getByLabelText('Third')).not.toBeChecked();
    });

    it('starts with the radio marked isChecked selected and forwards setIsChecked', () => {
        const setIsChecked = vi.fn();
        renderWithTheme(
            <RadioGroup
                radios={[
                    { label: 'First', isChecked: true },
                    { label: 'Second', setIsChecked },
                ]}
            />,
        );

        expect(screen.getByLabelText('First')).toBeChecked();

        fireEvent.click(screen.getByLabelText('Second'));

        expect(screen.getByLabelText('First')).not.toBeChecked();
        expect(setIsChecked).toHaveBeenCalledWith(true);
    });

    it('selects radios that have no label', () => {
        renderWithTheme(<RadioGroup radios={[{}, {}]} />);
        const [first, second] = screen.getAllByRole('radio');

        fireEvent.click(first);
        fireEvent.click(second);

        expect(first).not.toBeChecked();
        expect(second).toBeChecked();
    });
});

describe('Radio', () => {
    it('follows isChecked when a parent controls the selection', () => {
        const Controlled = () => {
            const [selected, setSelected] = useState('First');
            return (
                <>
                    {['First', 'Second'].map((label) => (
                        <Radio
                            key={label}
                            label={label}
                            name="controlled"
                            isChecked={selected === label}
                            setIsChecked={() => setSelected(label)}
                        />
                    ))}
                </>
            );
        };
        renderWithTheme(<Controlled />);

        fireEvent.click(screen.getByLabelText('Second'));

        expect(screen.getByLabelText('First')).not.toBeChecked();
        expect(screen.getByLabelText('Second')).toBeChecked();
    });
});
