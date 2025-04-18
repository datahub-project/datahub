import { Layout } from 'antd';
import React, { useEffect } from 'react';
import { Route, Switch, useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import DataHubTitle from '@app/DataHubTitle';
import EmbedRoutes from '@app/EmbedRoutes';
import { SearchRoutes } from '@app/SearchRoutes';
import { HomePage } from '@app/home/HomePage';
import { HomePage as HomePageV2 } from '@app/homeV2/HomePage';
import { IntroduceYourself } from '@app/homeV2/introduce/IntroduceYourself';
import { useSetUserPersona } from '@app/homeV2/persona/useUserPersona';
import { useSetUserTitle } from '@app/identity/user/useUserTitle';
import { OnboardingContextProvider } from '@app/onboarding/OnboardingContextProvider';
import { useIsThemeV2, useSetThemeIsV2 } from '@app/useIsThemeV2';
import { useSetNavBarRedesignEnabled } from '@app/useShowNavBarRedesign';
import { NEW_ROUTE_MAP, PageRoutes } from '@conf/Global';
import { getRedirectUrl } from '@conf/utils';

const StyledLayout = styled(Layout)`
    background-color: transparent;
`;

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    useSetThemeIsV2();
    useSetUserPersona();
    useSetUserTitle();
    useSetNavBarRedesignEnabled();

    const isThemeV2 = useIsThemeV2();
    const FinalHomePage = isThemeV2 ? HomePageV2 : HomePage;

    const location = useLocation();
    const history = useHistory();

    useEffect(() => {
        if (location.pathname.indexOf('/Validation') !== -1) {
            history.replace(getRedirectUrl(NEW_ROUTE_MAP));
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [location]);

    return (
        <OnboardingContextProvider>
            <DataHubTitle />
            <StyledLayout className={isThemeV2 ? 'themeV2' : undefined}>
                <Switch>
                    <Route exact path="/" render={() => <FinalHomePage />} />
                    <Route path={PageRoutes.EMBED} render={() => <EmbedRoutes />} />
                    <Route exact path={PageRoutes.INTRODUCE} render={() => <IntroduceYourself />} />
                    <Route path="/*" component={SearchRoutes} />
                </Switch>
            </StyledLayout>
        </OnboardingContextProvider>
    );
};
