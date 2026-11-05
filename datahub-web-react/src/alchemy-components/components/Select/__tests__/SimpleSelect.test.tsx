import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import themeV2 from '@conf/theme/themeV2';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

const OPTIONS = [
    { value: 'asset-a', label: 'Customer Orders' },
    { value: 'asset-b', label: 'Important Asset' },
];

beforeEach(() => {
    mockVisibilityObserver();
    vi.spyOn(window, 'requestAnimationFrame').mockImplementation((callback) => {
        callback(0);
        return 0;
    });
});

afterEach(() => {
    vi.restoreAllMocks();
});

function renderSelect(autoFocusSearch?: boolean) {
    render(
        <ThemeProvider theme={themeV2}>
            <SimpleSelect
                options={OPTIONS}
                placeholder="Select an asset"
                showSearch
                autoFocusSearch={autoFocusSearch}
            />
        </ThemeProvider>,
    );
}

function toggle() {
    act(() => {
        fireEvent.click(screen.getByText('Select an asset'));
    });
}

describe('SimpleSelect autoFocusSearch', () => {
    it('focuses the search box each time the dropdown opens', () => {
        renderSelect(true);

        toggle();
        expect(screen.getByTestId('dropdown-search-input')).toHaveFocus();

        act(() => {
            (document.activeElement as HTMLElement | null)?.blur();
        });
        toggle();
        toggle();
        expect(screen.getByTestId('dropdown-search-input')).toHaveFocus();
    });

    it('does not focus the search box by default', () => {
        renderSelect();

        toggle();
        expect(screen.getByTestId('dropdown-search-input')).not.toHaveFocus();
    });
});
