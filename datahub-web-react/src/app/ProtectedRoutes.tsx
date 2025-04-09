import React, { useEffect } from 'react';
import { Switch, Route, useLocation, useHistory } from 'react-router-dom';
import { Layout } from 'antd';
import styled from 'styled-components';
import DataHubTitle from './DataHubTitle';
import { HomePage } from './home/HomePage';
import { HomePage as HomePageV2 } from './homeV2/HomePage';
import { SearchRoutes } from './SearchRoutes';
import EmbedRoutes from './EmbedRoutes';
import { NEW_ROUTE_MAP, PageRoutes } from '../conf/Global';
import { useIsThemeV2, useSetThemeIsV2 } from './useIsThemeV2';
import { getRedirectUrl } from '../conf/utils';
import { IntroduceYourself } from './homeV2/introduce/IntroduceYourself';
import { useSetUserTitle } from './identity/user/useUserTitle';
import { useSetUserPersona } from './homeV2/persona/useUserPersona';
import { useSetNavBarRedesignEnabled } from './useShowNavBarRedesign';
import { OnboardingContextProvider } from './onboarding/OnboardingContextProvider';

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
