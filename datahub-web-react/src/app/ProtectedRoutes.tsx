import React from 'react';
import { Switch, Route } from 'react-router-dom';
import { Layout } from 'antd';
import { HomePage } from './home/HomePage';
import { SearchRoutes } from './SearchRoutes';
import { PageRoutes } from '../conf/Global';
import EmbeddedPage from './embed/EmbeddedPage';
import { useEntityRegistry } from './useEntityRegistry';
import AppProviders from './AppProviders';

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <AppProviders>
            <Layout style={{ height: '100%', width: '100%' }}>
                <Layout>
                    <Switch>
                        <Route exact path="/" render={() => <HomePage />} />
                        {entityRegistry.getEntities().map((entity) => (
                            <Route
                                key={`${entity.getPathName()}/${PageRoutes.EMBED}`}
                                path={`${PageRoutes.EMBED}/${entity.getPathName()}/:urn`}
                                render={() => <EmbeddedPage entityType={entity.type} />}
                            />
                        ))}
                        <Route path="/*" render={() => <SearchRoutes />} />
                    </Switch>
                </Layout>
            </Layout>
        </AppProviders>
    );
};
