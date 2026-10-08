import { act, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import UserContextProvider, { DEFAULT_VIEW_RESOLUTION_TIMEOUT_MS } from '@app/context/UserContextProvider';
import { useUserContext } from '@app/context/useUserContext';

type QueryResult = {
    data: unknown;
    error: Error | undefined;
    loading: boolean;
    called: boolean;
    refetch: () => void;
};

const harness = vi.hoisted(() => {
    const refetch = vi.fn();
    const me: QueryResult = { data: undefined, error: undefined, loading: false, called: false, refetch };
    const globalSettings: QueryResult = { data: undefined, error: undefined, loading: false, called: false, refetch };
    const getMe = vi.fn(() => {
        me.called = true;
    });
    const getGlobalViewsSettings = vi.fn(() => {
        globalSettings.called = true;
    });
    return { me, globalSettings, getMe, getGlobalViewsSettings };
});

vi.mock('@graphql/me.generated', () => ({
    useGetMeLazyQuery: () => [harness.getMe, harness.me],
}));

vi.mock('@graphql/app.generated', () => ({
    useGetGlobalViewsSettingsLazyQuery: () => [harness.getGlobalViewsSettings, harness.globalSettings],
}));

const PERSONAL_VIEW = 'urn:li:dataHubView:personal';
const STORED_VIEW = 'urn:li:dataHubView:stored';

function resetQueries() {
    harness.me.data = undefined;
    harness.me.error = undefined;
    harness.me.loading = false;
    harness.me.called = false;
    harness.globalSettings.data = undefined;
    harness.globalSettings.error = undefined;
    harness.globalSettings.loading = false;
    harness.globalSettings.called = false;
}

function Probe() {
    const { state, localState } = useUserContext();
    const viewUrn = localState.selectedViewUrn;
    let viewLabel = 'undefined';
    if (viewUrn === null) viewLabel = 'null';
    else if (viewUrn !== undefined) viewLabel = viewUrn;

    return (
        <div>
            <span data-testid="ready">{String(state.views.hasSetDefaultView)}</span>
            <span data-testid="view">{viewLabel}</span>
        </div>
    );
}

function renderProvider() {
    const view = render(
        <UserContextProvider>
            <Probe />
        </UserContextProvider>,
    );
    // The lookup hooks flip `called` inside an effect; rerender so settlement can run.
    view.rerender(
        <UserContextProvider>
            <Probe />
        </UserContextProvider>,
    );
    return view;
}

describe('UserContextProvider default view resolution', () => {
    beforeEach(() => {
        localStorage.clear();
        resetQueries();
        harness.getMe.mockClear();
        harness.getGlobalViewsSettings.mockClear();
    });

    it('marks resolution complete when both lookups return an empty payload and there is no default', () => {
        harness.me.data = {};
        harness.globalSettings.data = {};

        renderProvider();

        expect(screen.getByTestId('ready')).toHaveTextContent('true');
        expect(screen.getByTestId('view')).toHaveTextContent('undefined');
    });

    it('applies the personal default before search is allowed to run', () => {
        harness.me.data = {
            me: { corpUser: { settings: { views: { defaultView: { urn: PERSONAL_VIEW } } } } },
        };
        harness.globalSettings.data = { globalViewsSettings: { defaultView: 'urn:li:dataHubView:global' } };

        renderProvider();

        expect(screen.getByTestId('view')).toHaveTextContent(PERSONAL_VIEW);
        expect(screen.getByTestId('ready')).toHaveTextContent('true');
    });

    it('falls back to the global default when the user has none', () => {
        harness.me.data = { me: { corpUser: { settings: {} } } };
        harness.globalSettings.data = { globalViewsSettings: { defaultView: 'urn:li:dataHubView:global' } };

        renderProvider();

        expect(screen.getByTestId('view')).toHaveTextContent('urn:li:dataHubView:global');
        expect(screen.getByTestId('ready')).toHaveTextContent('true');
    });

    it('continues when getMe and global settings fail', () => {
        harness.me.error = new Error('me failed');
        harness.globalSettings.error = new Error('settings failed');

        renderProvider();

        expect(screen.getByTestId('ready')).toHaveTextContent('true');
        expect(screen.getByTestId('view')).toHaveTextContent('undefined');
    });

    it('does not replace a stored view, including an explicitly cleared one', () => {
        localStorage.setItem('userState', JSON.stringify({ selectedViewUrn: STORED_VIEW }));
        harness.me.data = {
            me: { corpUser: { settings: { views: { defaultView: { urn: PERSONAL_VIEW } } } } },
        };
        harness.globalSettings.data = {};

        renderProvider();

        expect(screen.getByTestId('view')).toHaveTextContent(STORED_VIEW);
        expect(screen.getByTestId('ready')).toHaveTextContent('true');
    });

    it('keeps an explicitly cleared view', () => {
        localStorage.setItem('userState', JSON.stringify({ selectedViewUrn: null }));
        harness.me.data = {
            me: { corpUser: { settings: { views: { defaultView: { urn: PERSONAL_VIEW } } } } },
        };
        harness.globalSettings.data = {};

        renderProvider();

        expect(screen.getByTestId('view')).toHaveTextContent('null');
        expect(screen.getByTestId('ready')).toHaveTextContent('true');
    });

    it('stops waiting after 10s when resolution never finishes', () => {
        vi.useFakeTimers();
        harness.me.loading = true;
        harness.globalSettings.loading = true;

        render(
            <UserContextProvider>
                <Probe />
            </UserContextProvider>,
        );

        expect(screen.getByTestId('ready')).toHaveTextContent('false');

        act(() => {
            vi.advanceTimersByTime(DEFAULT_VIEW_RESOLUTION_TIMEOUT_MS);
        });

        expect(screen.getByTestId('ready')).toHaveTextContent('true');
        expect(screen.getByTestId('view')).toHaveTextContent('undefined');
        vi.useRealTimers();
    });
});
