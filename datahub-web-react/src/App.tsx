import React from 'react';
import { BrowserRouter as Router } from 'react-router-dom';
import { ApolloClient, ApolloProvider, InMemoryCache } from '@apollo/client';
import { MockedProvider } from '@apollo/client/testing';
import './App.css';
import { Routes } from './components/Routes';
import { mocks } from './Mocks';

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
    return (
        <Router>
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
        </Router>
    );
};

export default App;
