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

describe('SimpleSelect empty state', () => {
    function search(query: string) {
        fireEvent.change(screen.getByTestId('dropdown-search-input'), { target: { value: query } });
    }

    it('shows the default no-results message when nothing matches', () => {
        renderSelect();

        toggle();
        search('nothing matches this');

        expect(screen.getByTestId('no-results-found')).toBeInTheDocument();
        expect(screen.getByText('No results found')).toBeInTheDocument();
        expect(screen.getByText("Try adjusting your search to find what you're looking for")).toBeInTheDocument();
    });

    it('renders a custom emptyState instead of the default', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <SimpleSelect
                    options={OPTIONS}
                    placeholder="Select an asset"
                    showSearch
                    emptyState={<div>Nothing here</div>}
                />
            </ThemeProvider>,
        );

        toggle();
        search('nothing matches this');

        expect(screen.getByText('Nothing here')).toBeInTheDocument();
        expect(screen.queryByTestId('no-results-found')).not.toBeInTheDocument();
    });
});
