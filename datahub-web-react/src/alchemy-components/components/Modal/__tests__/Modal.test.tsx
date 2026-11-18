import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { Modal } from '@components/components/Modal/Modal';

import themeV2 from '@conf/theme/themeV2';

describe('Modal', () => {
    it('explains a disabled footer button with its tooltip', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Modal
                    title="Create"
                    buttons={[{ text: 'Create', onClick: vi.fn(), disabled: true, tooltip: 'Missing permission' }]}
                />
            </ThemeProvider>,
        );

        const button = screen.getByRole('button', { name: 'Create' });
        expect(button).toBeDisabled();

        vi.useFakeTimers();
        fireEvent.mouseEnter(button.parentElement as HTMLElement);
        vi.advanceTimersByTime(100);

        expect(screen.getByRole('tooltip')).toHaveTextContent('Missing permission');
        vi.useRealTimers();
    });
});
