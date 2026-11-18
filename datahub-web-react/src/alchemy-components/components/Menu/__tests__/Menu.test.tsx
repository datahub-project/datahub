import { Menu } from '@components';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { ItemType } from '@components/components/Menu/types';

import themeV2 from '@conf/theme/themeV2';

const items: ItemType[] = [
    {
        type: 'item',
        key: 'parent',
        title: 'Parent',
        children: [{ type: 'custom', key: 'child', render: () => <span>Submenu content</span> }],
    },
];

function renderMenu(open: boolean) {
    return (
        <ThemeProvider theme={themeV2}>
            <Menu items={items} open={open}>
                <button type="button">trigger</button>
            </Menu>
        </ThemeProvider>
    );
}

const isSubmenuOpen = () =>
    screen
        .getByText('Parent')
        .closest('.ant-dropdown-menu-submenu')
        ?.classList.contains('ant-dropdown-menu-submenu-open');

describe('Menu', () => {
    it('closes an open submenu when the menu closes', async () => {
        const { rerender } = render(renderMenu(true));

        fireEvent.mouseEnter(screen.getByText('Parent').closest('.ant-dropdown-menu-submenu-title')!);
        await waitFor(() => expect(isSubmenuOpen()).toBe(true));

        rerender(renderMenu(false));
        rerender(renderMenu(true));

        expect(isSubmenuOpen()).toBe(false);
    });

    it('stays open when an item is clicked, leaving closing to the owner', () => {
        const onOpenChange = vi.fn();
        const onClick = vi.fn();
        render(
            <ThemeProvider theme={themeV2}>
                <Menu items={[{ type: 'item', key: 'item', title: 'Item', onClick }]} open onOpenChange={onOpenChange}>
                    <button type="button">trigger</button>
                </Menu>
            </ThemeProvider>,
        );

        fireEvent.click(screen.getByText('Item'));

        expect(onClick).toHaveBeenCalled();
        expect(onOpenChange).not.toHaveBeenCalledWith(false, expect.anything());
    });
});
