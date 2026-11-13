import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { TabButtons } from '@components/components/ButtonTabs/TabButtons';
import { TabButtonItem } from '@components/components/ButtonTabs/types';

import themeV2 from '@conf/theme/themeV2';

const TABS: TabButtonItem[] = [
    { key: 'a', label: 'Alpha' },
    { key: 'b', label: 'Beta', disabled: true, tooltip: 'Not available yet' },
];

function renderTabs(onTabClick: (key: string) => void) {
    return render(
        <ThemeProvider theme={themeV2}>
            <TabButtons tabs={TABS} activeTab="a" onTabClick={onTabClick} />
        </ThemeProvider>,
    );
}

describe('TabButtons', () => {
    it('does not select a disabled tab', () => {
        const onTabClick = vi.fn();
        renderTabs(onTabClick);

        const disabledTab = screen.getByRole('tab', { name: 'Beta' });
        expect(disabledTab).toBeDisabled();
        fireEvent.click(disabledTab);
        expect(onTabClick).not.toHaveBeenCalled();

        fireEvent.click(screen.getByRole('tab', { name: 'Alpha' }));
        expect(onTabClick).toHaveBeenCalledWith('a');
    });

    it('shows the tooltip when hovering a disabled tab', () => {
        vi.useFakeTimers();
        renderTabs(vi.fn());

        fireEvent.mouseEnter(screen.getByRole('tab', { name: 'Beta' }).parentElement as HTMLElement);
        act(() => {
            vi.advanceTimersByTime(200);
        });

        expect(screen.getByRole('tooltip')).toHaveTextContent('Not available yet');
        vi.useRealTimers();
    });
});
