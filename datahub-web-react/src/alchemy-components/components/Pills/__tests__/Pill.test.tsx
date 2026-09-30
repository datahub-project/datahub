import { X } from '@phosphor-icons/react/dist/csr/X';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { Pill } from '@components/components/Pills/Pill';

import CustomThemeProvider from '@src/CustomThemeProvider';

const REMOVE_LABEL = 'Remove';

function renderPill(props: Partial<React.ComponentProps<typeof Pill>>) {
    return render(
        <CustomThemeProvider>
            <Pill label="Chip" {...props} />
        </CustomThemeProvider>,
    );
}

describe('Pill keyboard behavior', () => {
    it('is exposed as a focusable button only when it has its own click action', () => {
        renderPill({ clickable: true, onPillClick: vi.fn() });
        const pill = screen.getByTestId('pill-container');
        expect(pill).toHaveAttribute('role', 'button');
        expect(pill).toHaveAttribute('tabindex', '0');
    });

    it('is not a tab stop when clickable only so an inner icon button can receive clicks', () => {
        renderPill({ clickable: true, rightIcon: X, onClickRightIcon: vi.fn() });
        const pill = screen.getByTestId('pill-container');
        expect(pill).not.toHaveAttribute('role');
        expect(pill).not.toHaveAttribute('tabindex');
        expect(screen.getByRole('button')).toBeInTheDocument();
    });

    it('activates onPillClick with Enter and Space on the pill itself', () => {
        const onPillClick = vi.fn();
        renderPill({ clickable: true, onPillClick });
        const pill = screen.getByTestId('pill-container');

        fireEvent.keyDown(pill, { key: 'Enter' });
        fireEvent.keyDown(pill, { key: ' ' });

        expect(onPillClick).toHaveBeenCalledTimes(2);
    });

    it('does not hijack Enter/Space from a nested icon button', () => {
        const onPillClick = vi.fn();
        renderPill({
            clickable: true,
            onPillClick,
            rightIcons: [{ icon: X, onClick: vi.fn(), ariaLabel: REMOVE_LABEL }],
        });
        const removeButton = screen.getByRole('button', { name: REMOVE_LABEL });

        const notPrevented = fireEvent.keyDown(removeButton, { key: 'Enter' });

        expect(notPrevented).toBe(true);
        expect(onPillClick).not.toHaveBeenCalled();
    });
});
