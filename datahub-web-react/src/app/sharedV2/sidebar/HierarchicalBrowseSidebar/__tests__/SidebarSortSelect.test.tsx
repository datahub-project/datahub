import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import SidebarSortSelect from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarSortSelect';
import darkTheme from '@conf/theme/colorThemes/dark';
import CustomThemeProvider from '@src/CustomThemeProvider';

const options = [
    { value: 'nameAsc', label: 'Name A to Z' },
    { value: 'nameDesc', label: 'Name Z to A' },
];

describe('SidebarSortSelect', () => {
    it('uses the semantic text color in dark mode', () => {
        render(
            <CustomThemeProvider isDarkMode injectGlobalStyles>
                <SidebarSortSelect options={options} value="nameAsc" onChange={vi.fn()} />
            </CustomThemeProvider>,
        );

        fireEvent.click(screen.getByTestId('sidebar-sort-select'));

        const label = screen.getByText('Name A to Z');
        expect(getComputedStyle(label).color).toBe(hexToRgb(darkTheme.text));
    });
});

function hexToRgb(hex: string) {
    const value = hex.replace('#', '');
    const red = parseInt(value.slice(0, 2), 16);
    const green = parseInt(value.slice(2, 4), 16);
    const blue = parseInt(value.slice(4, 6), 16);
    return `rgb(${red}, ${green}, ${blue})`;
}
