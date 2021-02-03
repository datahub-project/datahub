import React from 'react';
import { Switch, Route, RouteProps, Redirect } from 'react-router-dom';
import { useReactiveVar } from '@apollo/client';
import { BrowseTypesPage } from './browse/BrowseTypesPage';
import { BrowseResultsPage } from './browse/BrowseResultsPage';
import { SearchPage } from './search/SearchPage';
import { LogIn } from './auth/LogIn';
import { NoPageFound } from './shared/NoPageFound';
import { isLoggedInVar } from './auth/checkAuthStatus';
import { EntityPage } from './entity/EntityPage';
import { PageRoutes } from '../conf/Global';
import { useEntityRegistry } from './useEntityRegistry';

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
    const entityRegistry = useEntityRegistry();

    return (
        <div>
            <Switch>
                <ProtectedRoute isLoggedIn={isLoggedIn} exact path="/" render={() => <BrowseTypesPage />} />
                <Route path={PageRoutes.LOG_IN} component={LogIn} />

                {entityRegistry.getEntities().map((entity) => (
                    <ProtectedRoute
                        isLoggedIn={isLoggedIn}
                        path={`/${entity.getPathName()}/:urn`}
                        render={() => <EntityPage entityType={entity.type} />}
                    />
                ))}

                <ProtectedRoute isLoggedIn={isLoggedIn} path={PageRoutes.SEARCH} render={() => <SearchPage />} />
                <ProtectedRoute
                    isLoggedIn={isLoggedIn}
                    path={PageRoutes.SEARCH_RESULTS}
                    render={() => <SearchPage />}
                />
                <ProtectedRoute
                    isLoggedIn={isLoggedIn}
                    exact
                    path={PageRoutes.BROWSE}
                    render={() => <BrowseTypesPage />}
                />
                <ProtectedRoute
                    isLoggedIn={isLoggedIn}
                    path={PageRoutes.BROWSE_RESULTS}
                    render={() => <BrowseResultsPage />}
                />
                <Route component={NoPageFound} />
            </Switch>
        </div>
    );
};
