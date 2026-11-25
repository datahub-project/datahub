import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ViewSelector } from '@app/context/agents/ViewSelector';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

type View = { urn: string; name: string };
type ListVariables = { query?: string };

const SALES_VIEW: View = { urn: 'urn:li:dataHubView:sales', name: 'Sales' };
const FAR_VIEW: View = { urn: 'urn:li:dataHubView:far-away', name: 'Finance Reporting' };

const mockListMyViews = vi.fn();
const mockListGlobalViews = vi.fn();
const mockGetView = vi.fn();
vi.mock('@graphql/view.generated', () => ({
    useListMyViewsQuery: (options: { variables: ListVariables }) => mockListMyViews(options),
    useListGlobalViewsQuery: (options: { variables: ListVariables }) => mockListGlobalViews(options),
    useGetViewQuery: (options: { variables: { urn: string }; skip: boolean }) => mockGetView(options),
}));

function listResult(field: 'listMyViews' | 'listGlobalViews', views: View[]) {
    return { data: { [field]: { views } }, loading: false };
}

function renderSelector(value: string | undefined) {
    render(
        <CustomThemeProvider>
            <ViewSelector value={value} onChange={vi.fn()} />
        </CustomThemeProvider>,
    );
}

describe('ViewSelector', () => {
    beforeEach(() => {
        mockVisibilityObserver();
        vi.spyOn(window, 'requestAnimationFrame').mockImplementation((callback) => {
            callback(0);
            return 0;
        });
        mockListMyViews.mockReturnValue(listResult('listMyViews', []));
        mockListGlobalViews.mockReturnValue(listResult('listGlobalViews', [SALES_VIEW]));
        mockGetView.mockImplementation(({ skip }) =>
            skip ? { data: undefined, loading: false } : { data: { view: FAR_VIEW }, loading: false },
        );
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    it('shows the saved View by name when it is not in the listed Views', () => {
        renderSelector(FAR_VIEW.urn);

        expect(mockGetView).toHaveBeenLastCalledWith(
            expect.objectContaining({ variables: { urn: FAR_VIEW.urn }, skip: false }),
        );
        expect(screen.getByText(FAR_VIEW.name)).toBeInTheDocument();
    });

    it('does not look up the saved View when it is already listed', () => {
        renderSelector(SALES_VIEW.urn);

        expect(mockGetView).toHaveBeenLastCalledWith(expect.objectContaining({ skip: true }));
        expect(screen.getByText(SALES_VIEW.name)).toBeInTheDocument();
    });

    it('sends the search text to the server instead of filtering only the loaded Views', () => {
        vi.useFakeTimers();
        renderSelector(undefined);

        act(() => {
            fireEvent.click(screen.getByText('All data (no View)'));
        });
        fireEvent.change(screen.getByTestId('dropdown-search-input'), { target: { value: 'finance' } });
        act(() => {
            vi.advanceTimersByTime(500);
        });

        expect(mockListMyViews).toHaveBeenLastCalledWith(
            expect.objectContaining({ variables: expect.objectContaining({ query: 'finance' }) }),
        );
        expect(mockListGlobalViews).toHaveBeenLastCalledWith(
            expect.objectContaining({ variables: expect.objectContaining({ query: 'finance' }) }),
        );
    });
});
