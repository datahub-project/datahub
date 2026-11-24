import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it } from 'vitest';

import { Checkbox } from '@components/components/Checkbox';

import themeV2 from '@conf/theme/themeV2';

const renderCheckbox = (props: React.ComponentProps<typeof Checkbox>) =>
    render(
        <ThemeProvider theme={themeV2}>
            <Checkbox {...props} />
        </ThemeProvider>,
    );

describe('Checkbox', () => {
    it('reports partial selection as the native indeterminate state, not aria-checked', () => {
        renderCheckbox({ isIntermediate: true, 'aria-label': 'Select all' });

        const input = screen.getByRole('checkbox', { name: 'Select all' }) as HTMLInputElement;
        expect(input.indeterminate).toBe(true);
        expect(input).not.toHaveAttribute('aria-checked');
    });

    it('is named by its label', () => {
        renderCheckbox({ label: 'Include deprecated', isChecked: true });

        expect(screen.getByRole('checkbox', { name: 'Include deprecated' })).toBeChecked();
    });
});
