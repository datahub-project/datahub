import { Layout } from 'antd';
import React, { useEffect } from 'react';
import { Route, Switch, useHistory, useLocation } from 'react-router-dom';
import DataHubTitle from './DataHubTitle';
import AcrylRoutes from './AcrylRoutes';
import { HomePage } from './home/HomePage';
import { HomePage as HomePageV2 } from './homeV2/HomePage';
import { SearchRoutes } from './SearchRoutes';
import EmbedRoutes from './EmbedRoutes';
import { AcrylPageRoutes, PageRoutes } from '../conf/Global';
import { useIsThemeV2 } from './useIsThemeV2';
import { OnboardingContextProvider } from './onboarding/OnboardingContextProvider';
import { getRedirectUrl } from './shared/utils';

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    const isThemeV2 = useIsThemeV2();
    const FinalHomePage = isThemeV2 ? HomePageV2 : HomePage;

    const location = useLocation();
    const history = useHistory();

    useEffect(() => {
        if (location.pathname.indexOf('/Validation') !== -1) {
            const newRoutes = {
                '/Validation/Assertions': '/Quality/List',
                '/Validation/Tests': '/Governance/Tests',
                '/Validation/Data%20Contract': '/Quality/Data%20Contract',
                '/Validation': '/Quality',
            };
            history.replace(getRedirectUrl(newRoutes));
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [location]);

    return (
        <OnboardingContextProvider>
            <DataHubTitle />
            <Layout className={isThemeV2 ? 'themeV2' : undefined}>
                <Switch>
                    <Route exact path="/" render={() => <FinalHomePage />} />
                    <Route path={PageRoutes.EMBED} render={() => <EmbedRoutes />} />
                    <Route path={Object.values(AcrylPageRoutes)} component={AcrylRoutes} />
                    <Route path="/*" render={() => <SearchRoutes />} />
                </Switch>
            </Layout>
        </OnboardingContextProvider>
    );
};
