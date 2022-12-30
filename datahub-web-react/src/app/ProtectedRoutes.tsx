import React from 'react';
import { Switch, Route } from 'react-router-dom';
import { Layout } from 'antd';
import { HomePage } from './home/HomePage';
import AppConfigProvider from '../AppConfigProvider';
import { SearchRoutes } from './SearchRoutes';
import { EducationStepsProvider } from '../providers/EducationStepsProvider';
import UserContextProvider from './context/UserContextProvider';

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    return (
        <AppConfigProvider>
            <UserContextProvider>
                <EducationStepsProvider>
                    <Layout style={{ height: '100%', width: '100%' }}>
                        <Layout>
                            <Switch>
                                <Route exact path="/" render={() => <HomePage />} />
                                <Route path="/*" render={() => <SearchRoutes />} />
                            </Switch>
                        </Layout>
                    </Layout>
                </EducationStepsProvider>
            </UserContextProvider>
        </AppConfigProvider>
    );
};
