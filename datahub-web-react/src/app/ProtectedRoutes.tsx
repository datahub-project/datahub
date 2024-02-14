import { Layout } from 'antd';
import React from 'react';
import { Route, Switch } from 'react-router-dom';
import { HomePage } from './home/HomePage';
import { HomePage as HomePageV2 } from './homeV2/HomePage';
import { SearchRoutes } from './SearchRoutes';
import AppProviders from './AppProviders';
import EmbedRoutes from './EmbedRoutes';
import { PageRoutes } from '../conf/Global';
import { IntroduceYourself } from './homeV2/IntroduceYourself';
import { useIsThemeV2Enabled } from './useIsThemeV2Enabled';

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    const isThemeV2 = useIsThemeV2Enabled();
    const FinalHomePage = isThemeV2 ? HomePageV2 : HomePage;

    return (
        <AppProviders>
            <Layout className={isThemeV2 ? 'themeV2' : undefined}>
                <Switch>
                    <Route exact path="/" render={() => <FinalHomePage />} />
                    <Route exact path="/introduce" render={() => <IntroduceYourself />} />
                    <Route path={PageRoutes.EMBED} render={() => <EmbedRoutes />} />
                    <Route path="/*" render={() => <SearchRoutes />} />
                </Switch>
            </Layout>
        </AppProviders>
    );
};
