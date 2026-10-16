import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter, useLocation } from 'react-router-dom';

import { ProtectedRoutes } from '@app/ProtectedRoutes';

vi.mock('@app/DataHubTitle', () => ({ default: () => null }));
vi.mock('@app/EmbedRoutes', () => ({ default: () => null }));
vi.mock('@app/SearchRoutes', () => ({ SearchRoutes: () => <div data-testid="search-routes" /> }));
vi.mock('@app/homeV2/HomePage', () => ({ HomePage: () => <div data-testid="home-page" /> }));
vi.mock('@app/homeV2/introduce/IntroduceYourself', () => ({ IntroduceYourself: () => null }));
vi.mock('@app/homeV2/persona/useUserPersona', () => ({ useSetUserPersona: vi.fn() }));
vi.mock('@app/homeV3/HomePage', () => ({ HomePage: () => <div data-testid="home-page-v3" /> }));
vi.mock('@app/homeV3/context/hooks/useShowHomePageRedesign', () => ({ useShowHomePageRedesign: () => false }));
vi.mock('@app/identity/user/useUserTitle', () => ({ useSetUserTitle: vi.fn() }));
vi.mock('@app/onboarding/OnboardingContextProvider', () => ({
    OnboardingContextProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));
vi.mock('@app/useSetAppTheme', () => ({ useSetAppTheme: vi.fn() }));
vi.mock('@app/useShowNavBarRedesign', () => ({ useSetNavBarRedesignEnabled: vi.fn() }));

const CurrentPath = () => <div data-testid="current-path">{useLocation().pathname}</div>;

const renderRoutes = (path: string, landingPage = 'home') => {
    vi.stubEnv('REACT_APP_DEFAULT_LANDING_PAGE', landingPage);

    return render(
        <MemoryRouter initialEntries={[path]}>
            <CurrentPath />
            <ProtectedRoutes />
        </MemoryRouter>,
    );
};

describe('ProtectedRoutes default landing page', () => {
    afterEach(() => {
        vi.unstubAllEnvs();
    });

    it('keeps Home at the root by default', () => {
        renderRoutes('/');

        expect(screen.getByTestId('current-path')).toHaveTextContent('/');
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });

    it('redirects the root to Discover when configured', async () => {
        renderRoutes('/', 'discover');

        await waitFor(() => expect(screen.getByTestId('current-path')).toHaveTextContent('/search'));
        expect(screen.getByTestId('search-routes')).toBeInTheDocument();
    });

    it('keeps Home reachable at /home when Discover is configured', () => {
        renderRoutes('/home', 'discover');

        expect(screen.getByTestId('current-path')).toHaveTextContent('/home');
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });
});
