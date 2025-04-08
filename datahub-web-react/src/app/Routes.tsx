import { useReactiveVar } from '@apollo/client';
import React from 'react';
import { Route, RouteProps, Switch } from 'react-router-dom';

import AppProviders from '@app/AppProviders';
import { ProtectedRoutes } from '@app/ProtectedRoutes';
import { useTrackPageView } from '@app/analytics';
import { LogIn } from '@app/auth/LogIn';
import { ResetCredentials } from '@app/auth/ResetCredentials';
import { SignUp } from '@app/auth/SignUp';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import { NoPageFound } from '@app/shared/NoPageFound';
import { PageRoutes } from '@conf/Global';

const ProtectedRoute = ({
    isLoggedIn,
    ...props
}: {
    isLoggedIn: boolean;
} & RouteProps) => {
    const currentPath = window.location.pathname + window.location.search;
    if (!isLoggedIn) {
        window.location.replace(`${PageRoutes.AUTHENTICATE}?redirect_uri=${encodeURIComponent(currentPath)}`);
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
            <Route path={PageRoutes.LOG_IN} component={LogIn} />
            <Route path={PageRoutes.SIGN_UP} component={SignUp} />
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
