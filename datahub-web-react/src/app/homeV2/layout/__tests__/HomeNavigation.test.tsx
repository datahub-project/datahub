import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';

import { NavSidebar } from '@app/homeV2/layout/NavSidebar';
import AppLogoLink from '@app/shared/AppLogoLink';
import { PageRoutes } from '@conf/Global';
import themeV2 from '@conf/theme/themeV2';

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({
        urn: 'urn:li:corpuser:test',
        user: undefined,
        loaded: true,
    }),
}));
vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({ config: { visualConfig: { logoUrl: undefined } }, loaded: true }),
}));
vi.mock('@app/useEntityRegistry', () => ({ useEntityRegistry: () => ({ getPathName: () => 'corpuser' }) }));
vi.mock('@app/homeV2/layout/NavLinks', () => ({ NavLinks: () => null }));
vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (key: string) => key }) }));

const CurrentPath = () => <div data-testid="current-path">{useLocation().pathname}</div>;

const renderLink = (link: React.ReactNode) => {
    vi.stubEnv('REACT_APP_DEFAULT_LANDING_PAGE', 'discover');

    return render(
        <ThemeProvider theme={themeV2}>
            <MemoryRouter initialEntries={[PageRoutes.ROOT]}>
                {link}
                <CurrentPath />
            </MemoryRouter>
        </ThemeProvider>,
    );
};

describe('Home navigation links', () => {
    afterEach(() => {
        vi.unstubAllEnvs();
    });

    it('navigates the shared app logo to Home when Discover is the landing page', () => {
        renderLink(<AppLogoLink />);

        fireEvent.click(screen.getByRole('link'));

        expect(screen.getByTestId('current-path')).toHaveTextContent(PageRoutes.HOME);
    });

    it('navigates the legacy sidebar logo to Home when Discover is the landing page', () => {
        renderLink(<NavSidebar />);

        fireEvent.click(screen.getByTestId('datahub-logo-svg'));

        expect(screen.getByTestId('current-path')).toHaveTextContent(PageRoutes.HOME);
    });
});
