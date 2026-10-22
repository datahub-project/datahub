import { Layout } from 'antd';
import React, { Suspense, useEffect } from 'react';
import { Route, Switch, useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import DataHubTitle from '@app/DataHubTitle';
import { useSetUserPersona } from '@app/homeV2/persona/useUserPersona';
import { useShowHomePageRedesign } from '@app/homeV3/context/hooks/useShowHomePageRedesign';
import { useSetUserTitle } from '@app/identity/user/useUserTitle';
import { OnboardingContextProvider } from '@app/onboarding/OnboardingContextProvider';
import { SuspenseGlobal } from '@app/shared/SuspenseGlobal';
import { lazyDefault, lazyNamed } from '@app/shared/lazyPage';
import { useSetAppTheme } from '@app/useSetAppTheme';
import { useSetNavBarRedesignEnabled } from '@app/useShowNavBarRedesign';
import { NEW_ROUTE_MAP, PageRoutes } from '@conf/Global';
import { getRedirectUrl } from '@conf/utils';

const HomePageV2 = lazyNamed(() => import('@app/homeV2/HomePage'), 'HomePage');
const HomePageV3 = lazyNamed(() => import('@app/homeV3/HomePage'), 'HomePage');
const IntroduceYourself = lazyNamed(() => import('@app/homeV2/introduce/IntroduceYourself'), 'IntroduceYourself');
const SearchRoutes = lazyNamed(() => import('@app/SearchRoutes'), 'SearchRoutes');
const EmbedRoutes = lazyDefault(() => import('@app/EmbedRoutes'));

const StyledLayout = styled(Layout)`
    background-color: transparent;
`;

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    useSetAppTheme();
    useSetUserPersona();
    useSetUserTitle();
    useSetNavBarRedesignEnabled();

    const showHomepageRedesign = useShowHomePageRedesign();
    const FinalHomePage = showHomepageRedesign ? HomePageV3 : HomePageV2;

    const location = useLocation();
    const history = useHistory();

    useEffect(() => {
        if (location.pathname.indexOf('/Validation') !== -1) {
            history.replace(getRedirectUrl(NEW_ROUTE_MAP, location));
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [location]);

    return (
        <OnboardingContextProvider>
            <DataHubTitle />
            <StyledLayout className="themeV2">
                <Suspense fallback={<SuspenseGlobal />}>
                    <Switch>
                        <Route exact path="/" render={() => <FinalHomePage />} />
                        <Route path={PageRoutes.EMBED} render={() => <EmbedRoutes />} />
                        <Route exact path={PageRoutes.INTRODUCE} render={() => <IntroduceYourself />} />
                        <Route path="/*" component={SearchRoutes} />
                    </Switch>
                </Suspense>
            </StyledLayout>
        </OnboardingContextProvider>
    );
};
