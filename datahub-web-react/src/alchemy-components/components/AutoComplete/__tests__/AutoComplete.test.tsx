import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { DefaultTheme, ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import AutoComplete from '@components/components/AutoComplete/AutoComplete';
import theme from '@components/theme';

const OPTIONS = [{ value: 'alpha' }, { value: 'beta' }];

const renderAutoComplete = (onSelect = vi.fn()) =>
    render(
        <ThemeProvider theme={theme as unknown as DefaultTheme}>
            <AutoComplete options={OPTIONS} onSelect={onSelect}>
                <input aria-label="search" />
            </AutoComplete>
        </ThemeProvider>,
    );

describe('AutoComplete', () => {
    it('closes the dropdown on Tab without selecting an option or keeping focus', () => {
        const onSelect = vi.fn();
        renderAutoComplete(onSelect);
        const input = screen.getByRole('combobox');

        fireEvent.change(input, { target: { value: 'a' } });
        expect(input).toHaveAttribute('aria-expanded', 'true');

        const defaultAllowed = fireEvent.keyDown(input, { key: 'Tab', keyCode: 9, which: 9 });
        expect(defaultAllowed).toBe(true);
        expect(onSelect).not.toHaveBeenCalled();
        expect(input).toHaveAttribute('aria-expanded', 'false');
    });
});
