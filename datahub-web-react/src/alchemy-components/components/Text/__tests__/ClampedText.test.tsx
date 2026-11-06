import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { ClampedText } from '@components/components/Text/ClampedText';

import themeV2 from '@conf/theme/themeV2';

function mockHeights(scrollHeight: number, clientHeight: number) {
    vi.spyOn(HTMLElement.prototype, 'scrollHeight', 'get').mockReturnValue(scrollHeight);
    vi.spyOn(HTMLElement.prototype, 'clientHeight', 'get').mockReturnValue(clientHeight);
}

function renderWithTheme(node: React.ReactNode) {
    return render(<ThemeProvider theme={themeV2}>{node}</ThemeProvider>);
}

describe('ClampedText', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('does not show a toggle when the content fits', () => {
        mockHeights(40, 40);
        renderWithTheme(
            <ClampedText lineClamp={2} expandable>
                Short text
            </ClampedText>,
        );
        expect(screen.queryByRole('button')).not.toBeInTheDocument();
    });

    it('expands and collapses overflowing content with the default labels', () => {
        mockHeights(120, 40);
        const onExpand = vi.fn();
        const onCollapse = vi.fn();
        renderWithTheme(
            <ClampedText lineClamp={2} expandable={{ onExpand, onCollapse }}>
                Long text
            </ClampedText>,
        );

        fireEvent.click(screen.getByRole('button', { name: 'Read More' }));
        expect(onExpand).toHaveBeenCalledTimes(1);

        fireEvent.click(screen.getByRole('button', { name: 'Read Less' }));
        expect(onCollapse).toHaveBeenCalledTimes(1);
        expect(screen.getByRole('button', { name: 'Read More' })).toBeInTheDocument();
    });

    it('hides the toggle after expanding when not collapsible', () => {
        mockHeights(120, 40);
        renderWithTheme(
            <ClampedText lineClamp={2} expandable={{ expandLabel: 'More', collapsible: false }}>
                Long text
            </ClampedText>,
        );

        fireEvent.click(screen.getByRole('button', { name: 'More' }));
        expect(screen.queryByRole('button')).not.toBeInTheDocument();
    });

    it('never shows a toggle when not expandable', () => {
        mockHeights(120, 40);
        renderWithTheme(<ClampedText lineClamp={2}>Long text</ClampedText>);
        expect(screen.queryByRole('button')).not.toBeInTheDocument();
    });
});
