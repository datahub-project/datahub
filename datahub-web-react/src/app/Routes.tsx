import React from 'react';
import { Switch, Route, RouteProps, Redirect } from 'react-router-dom';
import { useReactiveVar } from '@apollo/client';
import { BrowseResultsPage } from './browse/BrowseResultsPage';
import { LogIn } from './auth/LogIn';
import { NoPageFound } from './shared/NoPageFound';
import { EntityPage } from './entity/EntityPage';
import { PageRoutes } from '../conf/Global';
import { useEntityRegistry } from './useEntityRegistry';
import { HomePage } from './home/HomePage';
import { SearchPage } from './search/SearchPage';
import { isLoggedInVar } from './auth/checkAuthStatus';

const ProtectedRoute = ({
    isLoggedIn,
    ...props
}: {
    isLoggedIn: boolean;
} & RouteProps) => {
    if (!isLoggedIn) {
        window.location.replace(PageRoutes.AUTHENTICATE);
        return null;
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
                <ProtectedRoute isLoggedIn={isLoggedIn} exact path="/" render={() => <HomePage />} />

                <Route path={PageRoutes.LOG_IN} component={LogIn} />
                {entityRegistry.getEntities().map((entity) => (
                    <ProtectedRoute
                        key={entity.getPathName()}
                        isLoggedIn={isLoggedIn}
                        path={`/${entity.getPathName()}/:urn`}
                        render={() => <EntityPage entityType={entity.type} />}
                    />
                ))}
                <ProtectedRoute
                    isLoggedIn={isLoggedIn}
                    path={PageRoutes.SEARCH_RESULTS}
                    render={() => <SearchPage />}
                />
                <ProtectedRoute
                    isLoggedIn={isLoggedIn}
                    path={PageRoutes.BROWSE_RESULTS}
                    render={() => <BrowseResultsPage />}
                />
                {/* Starting the react app locally opens /assets by default. For a smoother dev experience, we'll redirect to the homepage */}
                <Route path={PageRoutes.ASSETS} component={() => <Redirect to="/" />} exact />
                <Route component={NoPageFound} />
            </Switch>
        </div>
    );
};
