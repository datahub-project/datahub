import { Popover } from '@components';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it } from 'vitest';

import themeV2 from '@conf/theme/themeV2';

describe('Popover', () => {
    it('opens on click and closes with Escape', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Popover content={<button type="button">Popover action</button>} trigger="click">
                    <button type="button">Open details</button>
                </Popover>
            </ThemeProvider>,
        );

        fireEvent.click(screen.getByRole('button', { name: 'Open details' }));
        expect(screen.getByRole('dialog')).toContainElement(screen.getByRole('button', { name: 'Popover action' }));

        fireEvent.keyDown(document, { key: 'Escape' });
        expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    });
});
