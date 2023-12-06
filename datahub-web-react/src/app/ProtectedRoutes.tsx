import React from 'react';
import { Switch, Route } from 'react-router-dom';
import { Layout } from 'antd';
import { HomePage } from './home/HomePage';
import { SearchRoutes } from './SearchRoutes';
import { PageRoutes } from '../conf/Global';
import EmbeddedPage from './embed/EmbeddedPage';
import { useEntityRegistry } from './useEntityRegistry';
import AppProviders from './AppProviders';
import EmbedLookup from './embed/lookup';

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    return (
        <AppProviders>
            <Layout>
                <Switch>
                    <Route exact path="/" render={() => <HomePage />} />
                    <Route exact path={PageRoutes.EMBED_LOOKUP} render={() => <EmbedLookup />} />
                    {useEntityRegistry()
                        .getEntities()
                        .map((entity) => (
                            <Route
                                key={`${entity.getPathName()}/${PageRoutes.EMBED}`}
                                path={`${PageRoutes.EMBED}/${entity.getPathName()}/:urn`}
                                render={() => <EmbeddedPage entityType={entity.type} />}
                            />
                        ))}
                    <Route path="/*" render={() => <SearchRoutes />} />
                </Switch>
            </Layout>
        </AppProviders>
    );
};
