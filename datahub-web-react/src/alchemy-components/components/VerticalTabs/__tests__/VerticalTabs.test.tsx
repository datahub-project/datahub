import { ChartBar } from '@phosphor-icons/react/dist/csr/ChartBar';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { VerticalTabs } from '@components/components/VerticalTabs/VerticalTabs';
import { VerticalTabItem, VerticalTabsProps } from '@components/components/VerticalTabs/types';

import themeV2 from '@conf/theme/themeV2';

const ITEMS: VerticalTabItem[] = [
    { key: 'summary', label: 'Summary', icon: FileText, dataTestId: 'summary-tab' },
    { key: 'columns', label: 'Columns', icon: ListBullets },
    { key: 'stats', label: 'Stats', icon: ChartBar, disabled: true },
];

function renderTabs(props: Partial<VerticalTabsProps> = {}) {
    return render(
        <ThemeProvider theme={themeV2}>
            <VerticalTabs items={ITEMS} activeKey="summary" {...props} />
        </ThemeProvider>,
    );
}

describe('VerticalTabs', () => {
    it('renders a vertical tablist with one tab per item', () => {
        renderTabs();

        expect(screen.getByRole('tablist')).toHaveAttribute('aria-orientation', 'vertical');
        expect(screen.getAllByRole('tab').map((tab) => tab.getAttribute('aria-label'))).toEqual([
            'Summary',
            'Columns',
            'Stats',
        ]);
        expect(screen.getByTestId('summary-tab')).toBeInTheDocument();
    });

    it('marks only the active tab as selected, and none when activeKey is undefined', () => {
        const { rerender } = renderTabs();

        expect(screen.getByRole('tab', { name: 'Summary' })).toHaveAttribute('aria-selected', 'true');
        expect(screen.getByRole('tab', { name: 'Columns' })).toHaveAttribute('aria-selected', 'false');

        rerender(
            <ThemeProvider theme={themeV2}>
                <VerticalTabs items={ITEMS} activeKey={undefined} />
            </ThemeProvider>,
        );

        screen.getAllByRole('tab').forEach((tab) => expect(tab).toHaveAttribute('aria-selected', 'false'));
    });

    it('calls onChange only for a new tab and onTabClick for every click', () => {
        const onChange = vi.fn();
        const onTabClick = vi.fn();
        renderTabs({ onChange, onTabClick });

        fireEvent.click(screen.getByRole('tab', { name: 'Summary' }));
        fireEvent.click(screen.getByRole('tab', { name: 'Columns' }));

        expect(onTabClick.mock.calls).toEqual([['summary'], ['columns']]);
        expect(onChange.mock.calls).toEqual([['columns']]);
    });

    it('does not activate a disabled tab', () => {
        const onTabClick = vi.fn();
        renderTabs({ onTabClick });

        const disabledTab = screen.getByRole('tab', { name: 'Stats' });
        fireEvent.click(disabledTab);

        expect(disabledTab).toBeDisabled();
        expect(onTabClick).not.toHaveBeenCalled();
    });

    it('shows the item tooltip on hover', async () => {
        renderTabs({ items: [{ key: 'collapse', icon: FileText, tooltip: 'Collapse sidebar' }] });

        const tab = screen.getByRole('tab', { name: 'Collapse sidebar' });
        fireEvent.mouseEnter(tab);

        await waitFor(() => expect(screen.getByRole('tooltip')).toHaveTextContent('Collapse sidebar'));
    });

    it('moves focus with arrow keys, skipping disabled tabs', () => {
        renderTabs();

        const summary = screen.getByRole('tab', { name: 'Summary' });
        const columns = screen.getByRole('tab', { name: 'Columns' });
        expect(summary).toHaveAttribute('tabindex', '0');
        expect(columns).toHaveAttribute('tabindex', '-1');

        summary.focus();
        fireEvent.keyDown(summary, { key: 'ArrowDown' });
        expect(columns).toHaveFocus();

        fireEvent.keyDown(columns, { key: 'ArrowDown' });
        expect(summary).toHaveFocus();
    });

    it('prefixes tab ids with the rail id', () => {
        renderTabs({ id: 'entity-sidebar-tabs' });

        expect(screen.getByRole('tab', { name: 'Columns' })).toHaveAttribute('id', 'entity-sidebar-tabs-tab-columns');
    });
});
