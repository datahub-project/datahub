import { render, screen } from '@testing-library/react';
import React from 'react';
import { DefaultTheme, ThemeProvider } from 'styled-components';
import { describe, expect, it } from 'vitest';

import { DatePicker } from '@components/components/DatePicker/DatePicker';
import { DatePickerVariant } from '@components/components/DatePicker/constants';
import theme from '@components/theme';

import dayjs from '@utils/dayjs';

const VALUE = dayjs('2026-09-10T14:30:00');

const renderDatePicker = (props: Record<string, unknown> = {}) =>
    render(
        <ThemeProvider theme={theme as unknown as DefaultTheme}>
            <DatePicker value={VALUE} {...props} />
        </ThemeProvider>,
    );

describe('DatePicker format resolution', () => {
    it("uses the variant's format when neither format nor showTime is given", () => {
        renderDatePicker();

        expect(screen.getByRole('textbox')).toHaveValue('Sep 10, 2026');
    });

    it('uses an explicit format over the variant', () => {
        renderDatePicker({ format: 'DD/MM/YYYY' });

        expect(screen.getByRole('textbox')).toHaveValue('10/09/2026');
    });

    it("appends a time to the variant's format when showTime is enabled", () => {
        renderDatePicker({ showTime: true });

        expect(screen.getByRole('textbox')).toHaveValue('Sep 10, 2026 14:30:00');
    });

    it("appends a time to the EditableInput variant's display format", () => {
        renderDatePicker({ showTime: true, variant: DatePickerVariant.EditableInput });

        expect(screen.getByRole('textbox')).toHaveValue('Sep 10, 2026 14:30:00');
    });

    it('appends a time to an explicit date-only format', () => {
        renderDatePicker({ showTime: true, format: 'YYYY-MM-DD' });

        expect(screen.getByRole('textbox')).toHaveValue('2026-09-10 14:30:00');
    });

    it('leaves an explicit format that already carries a time', () => {
        renderDatePicker({ showTime: true, format: 'YYYY-MM-DD HH:mm' });

        expect(screen.getByRole('textbox')).toHaveValue('2026-09-10 14:30');
    });
});
