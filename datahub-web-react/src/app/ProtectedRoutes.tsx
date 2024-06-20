import { Layout } from 'antd';
import React from 'react';
import { Route, Switch } from 'react-router-dom';
import DataHubTitle from './DataHubTitle';
import AcrylRoutes from './AcrylRoutes';
import { HomePage } from './home/HomePage';
import { HomePage as HomePageV2 } from './homeV2/HomePage';
import { SearchRoutes } from './SearchRoutes';
import EmbedRoutes from './EmbedRoutes';
import { AcrylPageRoutes, PageRoutes } from '../conf/Global';
import { useIsThemeV2 } from './useIsThemeV2';
import { OnboardingContextProvider } from './onboarding/OnboardingContextProvider';

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    const isThemeV2 = useIsThemeV2();
    const FinalHomePage = isThemeV2 ? HomePageV2 : HomePage;

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
