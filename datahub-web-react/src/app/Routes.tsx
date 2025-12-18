import { useReactiveVar } from '@apollo/client';
import React from 'react';
import { Route, RouteProps, Switch, useLocation } from 'react-router-dom';

import AcrylApp from '@app/AcrylApp';
import AppProviders from '@app/AppProviders';
import { ProtectedRoutes } from '@app/ProtectedRoutes';
import { useTrackPageView } from '@app/analytics';
import { ImplicitLogIn } from '@app/auth/ImplicitLogIn';
import { ResetCredentials } from '@app/auth/ResetCredentials';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import LoginV2 from '@app/auth/loginV2/LoginV2';
import SignUpV2 from '@app/auth/signupV2/SignUpV2';
import { NoPageFound } from '@app/shared/NoPageFound';
import { ErrorSection } from '@app/shared/error/ErrorSection';
import { ErrorBoundary } from '@app/sharedV2/ErrorHandling/ErrorBoundary';
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
    const location = useLocation();
    useTrackPageView();
    const isLoggedIn = useReactiveVar(isLoggedInVar);

    return (
        <ErrorBoundary fallback={() => <ErrorSection />} resetKeys={[location.pathname]}>
            <Switch>
                <Route path={PageRoutes.LOG_IN} component={LoginV2} />
                <Route path={PageRoutes.IMPLICIT_LOG_IN} component={ImplicitLogIn} />
                <Route path={PageRoutes.SIGN_UP} component={SignUpV2} />
                <Route path={PageRoutes.RESET_CREDENTIALS} component={ResetCredentials} />
                <ProtectedRoute
                    isLoggedIn={isLoggedIn}
                    render={() => (
                        <AppProviders>
                            <AcrylApp>
                                <ProtectedRoutes />
                            </AcrylApp>
                        </AppProviders>
                    )}
                />
                <Route path="/*" component={NoPageFound} />
            </Switch>
        </ErrorBoundary>
    );
};
