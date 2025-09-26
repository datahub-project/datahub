import '@src/App.less';
import '@src/AppV2.less';

import { ApolloClient, ApolloProvider, InMemoryCache, ServerError, createHttpLink } from '@apollo/client';
import { onError } from '@apollo/client/link/error';
import Cookies from 'js-cookie';
import React, { useEffect } from 'react';
import { Helmet, HelmetProvider } from 'react-helmet-async';
import { BrowserRouter as Router } from 'react-router-dom';

import { Routes } from '@app/Routes';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import { ErrorCodes } from '@app/shared/constants';
import { PageRoutes } from '@conf/Global';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { GlobalCfg } from '@src/conf';
import { useCustomTheme } from '@src/customThemeContext';
import possibleTypesResult from '@src/possibleTypes.generated';
import { fixCSSFontPaths, getRuntimeBasePath, removeRuntimePath, resolveRuntimePath } from '@utils/runtimeBasePath';

/*
    Construct Apollo Client
*/
const httpLink = createHttpLink({
    uri: resolveRuntimePath(`/api/v2/graphql`),
});

const errorLink = onError((error) => {
    const { networkError } = error;
    if (networkError) {
        const serverError = networkError as ServerError;
        if (serverError.statusCode === ErrorCodes.Unauthorized) {
            isLoggedInVar(false);
            Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
            const currentPath = removeRuntimePath(window.location.pathname) + window.location.search;
            const authUrl = resolveRuntimePath(PageRoutes.AUTHENTICATE);
            window.location.replace(`${authUrl}?redirect_uri=${encodeURIComponent(currentPath)}`);
        }
    }
    // Disabled behavior for now -> Components are expected to handle their errors.
    // if (graphQLErrors && graphQLErrors.length) {
    //     const firstError = graphQLErrors[0];
    //     const { extensions } = firstError;
    //     const errorCode = extensions && (extensions.code as number);
    //     // Fallback in case the calling component does not handle.
    //     message.error(`${firstError.message} (code ${errorCode})`, 3); // TODO: Decide if we want this back.
    // }
});

const client = new ApolloClient({
    connectToDevTools: true,
    link: errorLink.concat(httpLink),
    cache: new InMemoryCache({
        typePolicies: {
            Query: {
                fields: {
                    dataset: {
                        merge: (oldObj, newObj) => {
                            return { ...oldObj, ...newObj };
                        },
                    },
                    entity: {
                        merge: (oldObj, newObj) => {
                            return { ...oldObj, ...newObj };
                        },
                    },
                },
            },
        },
        // need to define possibleTypes to allow us to use Apollo cache with union types
        possibleTypes: possibleTypesResult.possibleTypes,
    }),
    credentials: 'include',
    defaultOptions: {
        watchQuery: {
            fetchPolicy: 'no-cache',
        },
        query: {
            fetchPolicy: 'no-cache',
        },
    },
});

export const InnerApp: React.VFC = () => {
    // Fix CSS font paths after component mounts to ensure stylesheets are loaded
    useEffect(() => {
        // Use a small delay to ensure CSS is fully parsed and accessible
        const timer = setTimeout(() => {
            fixCSSFontPaths();
        }, 100);

        return () => clearTimeout(timer);
    }, []);

    return (
        <HelmetProvider>
            <CustomThemeProvider>
                <Helmet>
                    <title>{useCustomTheme().theme?.content?.title}</title>
                </Helmet>
                <Router basename={getRuntimeBasePath()}>
                    <Routes />
                </Router>
            </CustomThemeProvider>
        </HelmetProvider>
    );
};

export const App: React.VFC = () => {
    return (
        <ApolloProvider client={client}>
            <InnerApp />
        </ApolloProvider>
    );
};
