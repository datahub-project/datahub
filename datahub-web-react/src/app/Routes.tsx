import { useReactiveVar } from '@apollo/client';
import React from 'react';
import { Route, RouteProps, Switch, useLocation } from 'react-router-dom';

import AppProviders from '@app/AppProviders';
import { ProtectedRoutes } from '@app/ProtectedRoutes';
import { useTrackPageView } from '@app/analytics';
import { LogIn } from '@app/auth/LogIn';
import { ResetCredentials } from '@app/auth/ResetCredentials';
import { SignUp } from '@app/auth/SignUp';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import LoginV2 from '@app/auth/loginV2/LoginV2';
import SignUpV2 from '@app/auth/signupV2/SignUpV2';
import { useIngestionOnboardingRedesignV1 } from '@app/ingestV2/hooks/useIngestionOnboardingRedesignV1';
import { NoPageFound } from '@app/shared/NoPageFound';
import { PageRoutes } from '@conf/Global';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

const ProtectedRoute = ({
    isLoggedIn,
    ...props
}: {
    isLoggedIn: boolean;
} & RouteProps) => {
    const location = useLocation();
    const currentPath = location.pathname + location.search;
    if (!isLoggedIn) {
        // use window.location.replace to make an http request to frontend server, history.replace is for client-side navigation in React
        window.location.replace(
            `${resolveRuntimePath(PageRoutes.AUTHENTICATE)}?redirect_uri=${encodeURIComponent(currentPath)}`,
        );
        return null;
    }
    return <Route {...props} />;
};

/**
 * Container for all top-level routes.
 */
export const Routes = (): JSX.Element => {
    useTrackPageView();
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const showIngestionOnboardingRedesignV1 = useIngestionOnboardingRedesignV1();

    return (
        <Switch>
            <Route path={PageRoutes.LOG_IN} component={showIngestionOnboardingRedesignV1 ? LoginV2 : LogIn} />
            <Route path={PageRoutes.SIGN_UP} component={showIngestionOnboardingRedesignV1 ? SignUpV2 : SignUp} />
            <Route path={PageRoutes.RESET_CREDENTIALS} component={ResetCredentials} />
            <ProtectedRoute
                isLoggedIn={isLoggedIn}
                render={() => (
                    <AppProviders>
                        <ProtectedRoutes />
                    </AppProviders>
                )}
            />
            <Route path="/*" component={NoPageFound} />
        </Switch>
    );
};
