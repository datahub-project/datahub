import React, { useMemo } from 'react';
import { BrowserRouter as Router } from 'react-router-dom';
import { ApolloClient, ApolloProvider, InMemoryCache } from '@apollo/client';
import { MockedProvider } from '@apollo/client/testing';
import './App.css';
import { Routes } from './app/Routes';
import { mocks } from './Mocks';
import EntityRegistry from './app/entity/EntityRegistry';
import { DatasetEntity } from './app/entity/dataset/DatasetEntity';
import { UserEntity } from './app/entity/user/User';
import { EntityRegistryContext } from './entityRegistryContext';
import { DashboardEntity } from './app/entity/dashboard/DashboardEntity';
import { ChartEntity } from './app/entity/chart/ChartEntity';

// Enable to use the Apollo MockProvider instead of a real HTTP client
const MOCK_MODE = false;

/*
    Construct Apollo Client 
*/
const client = new ApolloClient({
    uri: '/api/v2/graphql',
    cache: new InMemoryCache({
        typePolicies: {
            Dataset: {
                keyFields: ['urn'],
            },
            CorpUser: {
                keyFields: ['urn'],
            },
            Dashboard: {
                keyFields: ['urn'],
            },
            Chart: {
                keyFields: ['urn'],
            },
        },
    }),
    credentials: 'include',
});

const App: React.VFC = () => {
    const entityRegistry = useMemo(() => {
        const register = new EntityRegistry();
        register.register(new DatasetEntity());
        register.register(new DashboardEntity());
        register.register(new ChartEntity());
        register.register(new UserEntity());
        return register;
    }, []);
    return (
        <Router>
            <EntityRegistryContext.Provider value={entityRegistry}>
                {/* Temporary: For local testing during development. */}
                {MOCK_MODE ? (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        <Routes />
                    </MockedProvider>
                ) : (
                    <ApolloProvider client={client}>
                        <Routes />
                    </ApolloProvider>
                )}
            </EntityRegistryContext.Provider>
        </Router>
    );
};

export default App;
