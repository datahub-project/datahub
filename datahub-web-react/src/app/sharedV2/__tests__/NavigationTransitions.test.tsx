import { act, render, screen } from '@testing-library/react';
import React, { Suspense, lazy } from 'react';
import { Link, MemoryRouter, Route, Switch } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it } from 'vitest';

import { NavigationTransitions, RouteSuspense } from '@app/sharedV2/NavigationTransitions';
import themeV2 from '@conf/theme/themeV2';

// Never resolves, so the lazy page stays loading for the whole test.
const neverLoads = () => lazy(() => new Promise<{ default: React.ComponentType }>(() => {}));

function renderApp({ withTransitions, routeBoundary }: { withTransitions: boolean; routeBoundary: boolean }) {
    const LazyPage = neverLoads();
    const page = routeBoundary ? (
        <RouteSuspense>
            <LazyPage />
        </RouteSuspense>
    ) : (
        <LazyPage />
    );
    const routes = (
        <Suspense fallback={<div>Loading app</div>}>
            <Switch>
                <Route exact path="/">
                    <Link to="/lazy">Go</Link>
                </Route>
                <Route path="/lazy">{page}</Route>
            </Switch>
        </Suspense>
    );
    render(
        <ThemeProvider theme={themeV2}>
            <MemoryRouter>
                {withTransitions ? <NavigationTransitions>{routes}</NavigationTransitions> : routes}
            </MemoryRouter>
        </ThemeProvider>,
    );
}

describe('NavigationTransitions', () => {
    it('keeps the current page while a lazy route loads', async () => {
        renderApp({ withTransitions: true, routeBoundary: false });

        await act(async () => screen.getByText('Go').click());

        expect(screen.getByText('Go')).toBeVisible();
        expect(screen.queryByText('Loading app')).not.toBeInTheDocument();
    });

    it('keeps the current page when the route mounts its own boundary', async () => {
        renderApp({ withTransitions: true, routeBoundary: true });

        await act(async () => screen.getByText('Go').click());

        expect(screen.getByText('Go')).toBeVisible();
        expect(screen.queryByLabelText('Loading...')).not.toBeInTheDocument();
    });

    it('shows the fallback without it', async () => {
        renderApp({ withTransitions: false, routeBoundary: false });

        await act(async () => screen.getByText('Go').click());

        expect(screen.getByText('Loading app')).toBeInTheDocument();
    });

    it('shows the route spinner outside a navigation', () => {
        const LazyPage = neverLoads();
        render(
            <ThemeProvider theme={themeV2}>
                <MemoryRouter>
                    <NavigationTransitions>
                        <RouteSuspense>
                            <LazyPage />
                        </RouteSuspense>
                    </NavigationTransitions>
                </MemoryRouter>
            </ThemeProvider>,
        );

        expect(screen.getByLabelText('Loading...')).toBeInTheDocument();
    });
});
