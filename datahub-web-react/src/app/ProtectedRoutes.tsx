import React, { useEffect } from 'react';
import { Switch, Route, useLocation, useHistory } from 'react-router-dom';
import { Layout } from 'antd';
import { HomePage } from './home/HomePage';
import { SearchRoutes } from './SearchRoutes';
import EmbedRoutes from './EmbedRoutes';
import { PageRoutes } from '../conf/Global';
import { getRedirectUrl } from './shared/utils';

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    const location = useLocation();
    const history = useHistory();

    useEffect(() => {
        if (location.pathname.indexOf('/Validation') !== -1) {
            const newRoutes = {
                '/Validation/Assertions': '/Quality/List',
                '/Validation/Tests': '/Governance/Tests',
                '/Validation/Data%20Contract': '/Quality/Data%20Contract',
                '/Validation': '/Quality',
            };
            history.replace(getRedirectUrl(newRoutes));
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [location]);

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
