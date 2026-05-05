import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { Alert } from '@components/components/Alert/Alert';
import { AlertProps, AlertVariant } from '@components/components/Alert/types';

import themeV2 from '@conf/theme/themeV2';

function renderAlert(props: Partial<AlertProps> & { variant?: AlertVariant; title?: string } = {}) {
    const defaultProps: AlertProps = {
        variant: 'success',
        title: 'Test title',
        ...props,
    };

    return render(
        <ThemeProvider theme={themeV2}>
            <Alert {...defaultProps} />
        </ThemeProvider>,
    );
}

describe('Alert', () => {
    it('should render the title', () => {
        renderAlert({ title: 'Operation complete' });

        expect(screen.getByText('Operation complete')).toBeInTheDocument();
    });

    it('should render the description when provided', () => {
        renderAlert({ title: 'Done', description: 'All changes saved.' });

        expect(screen.getByText('Done')).toBeInTheDocument();
        expect(screen.getByText('All changes saved.')).toBeInTheDocument();
    });

    it('should not render a description when omitted', () => {
        renderAlert({ title: 'Title only' });

        expect(screen.getByText('Title only')).toBeInTheDocument();
        expect(screen.queryByText('All changes saved.')).not.toBeInTheDocument();
    });

    it('should render an action element when provided', () => {
        renderAlert({
            title: 'Error',
            variant: 'error',
            action: <button type="button">Retry</button>,
        });

        expect(screen.getByText('Retry')).toBeInTheDocument();
    });

    it('should render a close button when onClose is provided', () => {
        const onClose = vi.fn();
        renderAlert({ title: 'Dismissable', onClose });

        const closeButton = screen.getByRole('button', { name: /close/i });
        expect(closeButton).toBeInTheDocument();

        fireEvent.click(closeButton);
        expect(onClose).toHaveBeenCalledOnce();
    });

    it('should not render close button when onClose is omitted', () => {
        renderAlert({ title: 'No close' });

        expect(screen.queryByRole('button', { name: /close/i })).not.toBeInTheDocument();
    });

    it.each<AlertVariant>(['success', 'error', 'warning', 'info', 'brand'])(
        'should render without errors for variant "%s"',
        (variant) => {
            const { container } = renderAlert({ variant, title: `${variant} alert` });

            expect(screen.getByText(`${variant} alert`)).toBeInTheDocument();
            expect(container.firstChild).toBeInTheDocument();
        },
    );

    it('should apply className and style props', () => {
        const { container } = renderAlert({
            title: 'Styled',
            className: 'custom-class',
            style: { marginTop: 12 },
        });

        const alertEl = container.firstChild as HTMLElement;
        expect(alertEl.classList.contains('custom-class')).toBe(true);
        expect(alertEl.style.marginTop).toBe('12px');
    });
});
