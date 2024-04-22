import React from 'react';
import { Switch, Route } from 'react-router-dom';
import { Layout } from 'antd';
import { HomePage } from './home/HomePage';
import { SearchRoutes } from './SearchRoutes';
import EmbedRoutes from './EmbedRoutes';
import { PageRoutes } from '../conf/Global';

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    return (
        <Layout>
            <Switch>
                <Route exact path="/" render={() => <HomePage />} />
                <Route path={PageRoutes.EMBED} render={() => <EmbedRoutes />} />
                <Route path="/*" render={() => <SearchRoutes />} />
            </Switch>
        </Layout>
    );
};
