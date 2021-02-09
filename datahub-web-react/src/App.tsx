import React, { useMemo } from 'react';
import { BrowserRouter as Router } from 'react-router-dom';
import { ApolloClient, ApolloProvider, InMemoryCache } from '@apollo/client';
import { MockedProvider } from '@apollo/client/testing';
import './App.css';
import { Routes } from './components/Routes';
import { mocks } from './Mocks';
import EntityRegistry from './components/entity/EntityRegistry';
import { DatasetEntity } from './components/entity/dataset/DatasetEntity';
import { UserEntity } from './components/entity/user/User';
import { EntityRegistryContext } from './entityRegistryContext';

// Enable to use the Apollo MockProvider instead of a real HTTP client
const MOCK_MODE = true;

/*
    Construct Apollo Client 
*/
const client = new ApolloClient({
    uri: 'http://localhost:9001/api/v2/graphql',
    cache: new InMemoryCache({
        typePolicies: {
            Dataset: {
                keyFields: ['urn'], // TODO: Set this as the default across the app.
            },
        },
    }),
    credentials: 'include',
});

const App: React.VFC = () => {
    // TODO: Explore options to dynamically configure this.
    const entityRegistry = useMemo(() => {
        const register = new EntityRegistry();
        register.register(new DatasetEntity());
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
