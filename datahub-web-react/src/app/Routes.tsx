import { useReactiveVar } from '@apollo/client';
import React from 'react';
import { Route, RouteProps, Switch, useLocation } from 'react-router-dom';

import AppProviders from '@app/AppProviders';
import { ProtectedRoutes } from '@app/ProtectedRoutes';
import { useTrackPageView } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import LoginV2 from '@app/auth/loginV2/LoginV2';
import ResetCredentialsV2 from '@app/auth/resetCredentialsV2/ResetCredentialsV2';
import SignUpV2 from '@app/auth/signupV2/SignUpV2';
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

    return (
        <Switch>
            <Route path={PageRoutes.LOG_IN} component={LoginV2} />
            <Route path={PageRoutes.SIGN_UP} component={SignUpV2} />
            <Route path={PageRoutes.RESET_CREDENTIALS} component={ResetCredentialsV2} />
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
