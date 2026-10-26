import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it } from 'vitest';

import { Input } from '@components/components/Input/Input';

import themeV2 from '@conf/theme/themeV2';

function renderInput(props: React.ComponentProps<typeof Input> = {}) {
    return render(
        <ThemeProvider theme={themeV2}>
            <Input label="Title" inputTestId="title-input" {...props} />
        </ThemeProvider>,
    );
}

describe('Input', () => {
    it('focuses the field when autoFocus is set', () => {
        renderInput({ autoFocus: true });

        expect(screen.getByTestId('title-input')).toHaveFocus();
    });

    it('leaves the field unfocused by default', () => {
        renderInput();

        expect(screen.getByTestId('title-input')).not.toHaveFocus();
    });
});
