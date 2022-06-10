import React from 'react';
import { Switch, Route, RouteProps, Redirect } from 'react-router-dom';
import { useReactiveVar } from '@apollo/client';
import { LogIn } from './auth/LogIn';
import { SignUp } from './auth/SignUp';
import { ResetCredentials } from './auth/ResetCredentials';
import { NoPageFound } from './shared/NoPageFound';
import { PageRoutes } from '../conf/Global';
import { isLoggedInVar } from './auth/checkAuthStatus';
import { useTrackPageView } from './analytics';
import { ProtectedRoutes } from './ProtectedRoutes';

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
            <ProtectedRoute isLoggedIn={isLoggedIn} render={() => <ProtectedRoutes />} />
            {/* Starting the react app locally opens /assets by default. For a smoother dev experience, we'll redirect to the homepage */}
            <Route path={PageRoutes.ASSETS} component={() => <Redirect to="/" />} exact />
            <Route component={NoPageFound} />
        </Switch>
    );
};
