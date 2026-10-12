import { useReactiveVar } from '@apollo/client';
import React, { Suspense } from 'react';
import { Route, RouteProps, Switch, useLocation } from 'react-router-dom';

import { useTrackPageView } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import { NoPageFound } from '@app/shared/NoPageFound';
import { SuspenseGlobal } from '@app/shared/SuspenseGlobal';
import { lazyDefault } from '@app/shared/lazyPage';
import { PageRoutes } from '@conf/Global';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

const LoginV2 = lazyDefault(() => import('@app/auth/loginV2/LoginV2'));
const SignUpV2 = lazyDefault(() => import('@app/auth/signupV2/SignUpV2'));
const ResetCredentialsV2 = lazyDefault(() => import('@app/auth/resetCredentialsV2/ResetCredentialsV2'));
const AuthenticatedApp = lazyDefault(() => import('@app/AuthenticatedApp'));

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
        <Suspense fallback={<SuspenseGlobal />}>
            <Switch>
                <Route path={PageRoutes.LOG_IN} component={LoginV2} exact />
                <Route path={PageRoutes.SIGN_UP} component={SignUpV2} />
                <Route path={PageRoutes.RESET_CREDENTIALS} component={ResetCredentialsV2} />
                <ProtectedRoute isLoggedIn={isLoggedIn} render={() => <AuthenticatedApp />} />
                <Route path="/*" component={NoPageFound} />
            </Switch>
        </Suspense>
    );
};
