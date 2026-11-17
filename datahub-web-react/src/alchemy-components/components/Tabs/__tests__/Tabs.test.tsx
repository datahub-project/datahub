import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { Tab, TabErrorBoundaryProps, Tabs } from '@components/components/Tabs/Tabs';

import themeV2 from '@conf/theme/themeV2';

function renderTabs(tabs: Tab[], onTabClick?: (key: string) => void) {
    return render(
        <ThemeProvider theme={themeV2}>
            <Tabs tabs={tabs} selectedTab={tabs[0]?.key} onTabClick={onTabClick} />
        </ThemeProvider>,
    );
}

const BASE_TAB: Tab = {
    key: 'first',
    name: 'First tab',
    component: <div>First content</div>,
};

describe('Tabs', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('opens a context menu on right click and runs the selected item', () => {
        const onClick = vi.fn();
        renderTabs([
            {
                ...BASE_TAB,
                contextMenuItems: [{ type: 'item', key: 'copy', title: 'Copy something', onClick }],
            },
        ]);

        expect(screen.queryByText('Copy something')).not.toBeInTheDocument();

        fireEvent.contextMenu(screen.getByText('First tab'));
        fireEvent.click(screen.getByText('Copy something'));

        expect(onClick).toHaveBeenCalledTimes(1);
    });

    it('still shows the tooltip on a tab that has context menu items', async () => {
        renderTabs([
            {
                ...BASE_TAB,
                tooltip: 'Tab tooltip',
                contextMenuItems: [{ type: 'item', key: 'copy', title: 'Copy something' }],
            },
        ]);

        fireEvent.mouseEnter(screen.getByText('First tab'));

        await waitFor(() => expect(screen.getByText('Tab tooltip')).toBeInTheDocument());
    });

    it('does not select the tab when its context menu item is clicked', () => {
        const onTabClick = vi.fn();
        renderTabs(
            [
                BASE_TAB,
                {
                    key: 'second',
                    name: 'Second tab',
                    component: <div>Second content</div>,
                    contextMenuItems: [{ type: 'item', key: 'copy', title: 'Copy something' }],
                },
            ],
            onTabClick,
        );

        fireEvent.contextMenu(screen.getByText('Second tab'));
        fireEvent.click(screen.getByText('Copy something'));

        expect(onTabClick).not.toHaveBeenCalled();

        fireEvent.click(screen.getByText('Second tab'));

        expect(onTabClick).toHaveBeenCalledWith('second', expect.anything());
    });

    it('closes the context menu on an outside click', async () => {
        render(
            <ThemeProvider theme={themeV2}>
                <div>
                    <button type="button">outside</button>
                    <Tabs
                        selectedTab={BASE_TAB.key}
                        tabs={[
                            {
                                ...BASE_TAB,
                                contextMenuItems: [{ type: 'item', key: 'copy', title: 'Copy something' }],
                            },
                        ]}
                    />
                </div>
            </ThemeProvider>,
        );

        fireEvent.contextMenu(screen.getByText('First tab'));
        expect(screen.getByText('Copy something')).toBeInTheDocument();

        fireEvent.mouseDown(screen.getByText('outside'));

        await waitFor(() => expect(screen.queryByText('Copy something')).not.toBeInTheDocument());
    });

    it('does not open a context menu when the tab has no items', () => {
        renderTabs([BASE_TAB]);

        fireEvent.contextMenu(screen.getByText('First tab'));

        expect(screen.queryByRole('menu')).not.toBeInTheDocument();
    });

    it('wraps each tab content in the given error boundary with its reset keys', () => {
        const boundary = vi.fn(({ resetKeys, children }: TabErrorBoundaryProps) => (
            <div data-testid="tab-boundary" data-reset-keys={resetKeys.join(',')}>
                {children}
            </div>
        ));
        render(
            <ThemeProvider theme={themeV2}>
                <Tabs
                    tabs={[{ ...BASE_TAB, errorResetKey: 'urn:1' }]}
                    selectedTab={BASE_TAB.key}
                    tabErrorBoundary={boundary}
                />
            </ThemeProvider>,
        );

        const wrapper = screen.getByTestId('tab-boundary');
        expect(wrapper).toHaveAttribute('data-reset-keys', 'first,urn:1');
        expect(wrapper).toHaveTextContent('First content');
    });

    it('lets a tab error propagate when no error boundary is provided', () => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
        const Broken = () => {
            throw new Error('tab crashed');
        };

        expect(() => renderTabs([{ ...BASE_TAB, component: <Broken /> }])).toThrow('tab crashed');
    });
});
