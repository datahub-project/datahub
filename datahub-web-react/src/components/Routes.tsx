import React from 'react';
import { Switch, Route, RouteProps, Redirect } from 'react-router-dom';
import { useReactiveVar } from '@apollo/client';
import { BrowseTypesPage } from './browse/BrowseTypesPage';
import { BrowseResultsPage } from './browse/BrowseResultsPage';
import { DatasetPage } from './entity/dataset/DatasetPage';
import { UserPage } from './entity/user/UserPage';
import { SearchPage } from './search/SearchPage';
import { LogIn } from './auth/LogIn';
import { NoPageFound } from './shared/NoPageFound';
import { isLoggedInVar } from './auth/checkAuthStatus';
import { PageRoutes } from '../conf/Global';

const ProtectedRoute = ({
    isLoggedIn,
    ...props
}: {
    isLoggedIn: boolean;
} & RouteProps) => {
    if (!isLoggedIn) {
        return <Redirect to={PageRoutes.LOG_IN} />;
    }
    return <Route {...props} />;
};

/**
 * Container for all views behind an authentication wall.
 */
export const Routes = (): JSX.Element => {
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    return (
        <div>
            <Switch>
                <ProtectedRoute isLoggedIn={isLoggedIn} exact path="/" render={() => <BrowseTypesPage />} />
                <Route path={PageRoutes.LOG_IN} component={LogIn} />
                <ProtectedRoute
                    isLoggedIn={isLoggedIn}
                    path={`${PageRoutes.DATASETS}/:urn`}
                    render={() => <DatasetPage />}
                />
                <ProtectedRoute isLoggedIn={isLoggedIn} path={PageRoutes.SEARCH} render={() => <SearchPage />} />
                <ProtectedRoute
                    isLoggedIn={isLoggedIn}
                    exact
                    path={PageRoutes.BROWSE_TYPES}
                    render={() => <BrowseTypesPage />}
                />
                <ProtectedRoute
                    isLoggedIn={isLoggedIn}
                    path={PageRoutes.BROWSE_RESULTS}
                    render={() => <BrowseResultsPage />}
                />
                <ProtectedRoute isLoggedIn={isLoggedIn} path={PageRoutes.USERS} render={() => <UserPage />} />
                <Route component={NoPageFound} />
            </Switch>
        </div>
    );
};
