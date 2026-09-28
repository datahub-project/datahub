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

    it('renders an always-open popover without a child trigger', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Popover open content="Series value" />
            </ThemeProvider>,
        );

        expect(screen.getByRole('dialog')).toHaveTextContent('Series value');
    });

    it('lets overlayStyle pin the position', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Popover open content="Pinned" overlayStyle={{ left: 0, width: '100%' }}>
                    <button type="button">Trigger</button>
                </Popover>
            </ThemeProvider>,
        );

        const dialog = screen.getByRole('dialog');
        expect(dialog.style.left).toBe('0px');
        expect(dialog.style.width).toBe('100%');
        expect(dialog.style.transform).toBe('');
    });

    it('stays open when the trigger blurs into the overlay', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Popover content={<button type="button">Inside</button>}>
                    <button type="button">Trigger</button>
                </Popover>
            </ThemeProvider>,
        );

        const trigger = screen.getByRole('button', { name: 'Trigger' });
        fireEvent.focus(trigger);
        const inside = screen.getByRole('button', { name: 'Inside' });

        fireEvent.blur(trigger, { relatedTarget: inside });
        expect(screen.getByRole('dialog')).toBeInTheDocument();

        fireEvent.mouseDown(screen.getByRole('dialog'));
        fireEvent.blur(trigger, { relatedTarget: null });
        expect(screen.getByRole('dialog')).toBeInTheDocument();

        fireEvent.mouseUp(document);
        fireEvent.blur(trigger, { relatedTarget: null });
        expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    });

    it('ignores focus when only hover is requested', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Popover content="Hover only" trigger="hover">
                    <button type="button">Trigger</button>
                </Popover>
            </ThemeProvider>,
        );

        fireEvent.focus(screen.getByRole('button', { name: 'Trigger' }));
        expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    });
});
