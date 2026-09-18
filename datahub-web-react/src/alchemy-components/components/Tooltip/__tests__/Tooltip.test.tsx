import { Tooltip } from '@components';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it } from 'vitest';

import themeV2 from '@conf/theme/themeV2';

function renderTooltip(title: React.ReactNode, open?: boolean) {
    return render(
        <ThemeProvider theme={themeV2}>
            <Tooltip title={title} open={open}>
                <button type="button">Target</button>
            </Tooltip>
        </ThemeProvider>,
    );
}

describe('Tooltip', () => {
    it('shows accessible content when the target receives focus', () => {
        renderTooltip('Helpful details');

        fireEvent.focus(screen.getByRole('button', { name: 'Target' }));

        expect(screen.getByRole('tooltip')).toHaveTextContent('Helpful details');
    });

    it('does not create an overlay without content', () => {
        renderTooltip(undefined);

        fireEvent.focus(screen.getByRole('button', { name: 'Target' }));

        expect(screen.queryByRole('tooltip')).not.toBeInTheDocument();
    });

    it('supports controlled visibility', () => {
        renderTooltip('Always visible', true);

        expect(screen.getByRole('tooltip')).toHaveTextContent('Always visible');
    });

    it('clamps plain text descriptions', () => {
        renderTooltip('A very long description'.repeat(50), true);

        expect(screen.getByRole('tooltip').querySelector('.alchemy-floating-overlay-clamp')).toBeInTheDocument();
    });

    it('leaves structured content unclamped unless maxLines is given', () => {
        renderTooltip(
            <div>
                <span>Title</span>
                <span>Value</span>
            </div>,
            true,
        );

        expect(screen.getByRole('tooltip').querySelector('.alchemy-floating-overlay-clamp')).not.toBeInTheDocument();
    });
});
