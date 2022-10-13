import React, { useEffect, useMemo, useState } from 'react';
import Cookies from 'js-cookie';
import { message } from 'antd';
import { BrowserRouter as Router } from 'react-router-dom';
import { ApolloClient, ApolloProvider, createHttpLink, InMemoryCache, ServerError } from '@apollo/client';
import { onError } from '@apollo/client/link/error';
import { ThemeProvider } from 'styled-components';
import './App.less';
import { Routes } from './app/Routes';
import EntityRegistry from './app/entity/EntityRegistry';
import { DashboardEntity } from './app/entity/dashboard/DashboardEntity';
import { ChartEntity } from './app/entity/chart/ChartEntity';
import { UserEntity } from './app/entity/user/User';
import { GroupEntity } from './app/entity/group/Group';
import { DatasetEntity } from './app/entity/dataset/DatasetEntity';
import { DataFlowEntity } from './app/entity/dataFlow/DataFlowEntity';
import { DataJobEntity } from './app/entity/dataJob/DataJobEntity';
import { TagEntity } from './app/entity/tag/Tag';
import { EntityRegistryContext } from './entityRegistryContext';
import { Theme } from './conf/theme/types';
import defaultThemeConfig from './conf/theme/theme_light.config.json';
import { PageRoutes } from './conf/Global';
import { isLoggedInVar } from './app/auth/checkAuthStatus';
import { GlobalCfg } from './conf';
import { GlossaryTermEntity } from './app/entity/glossaryTerm/GlossaryTermEntity';
import { MLFeatureEntity } from './app/entity/mlFeature/MLFeatureEntity';
import { MLPrimaryKeyEntity } from './app/entity/mlPrimaryKey/MLPrimaryKeyEntity';
import { MLFeatureTableEntity } from './app/entity/mlFeatureTable/MLFeatureTableEntity';
import { MLModelEntity } from './app/entity/mlModel/MLModelEntity';
import { MLModelGroupEntity } from './app/entity/mlModelGroup/MLModelGroupEntity';
import { DomainEntity } from './app/entity/domain/DomainEntity';
import { ContainerEntity } from './app/entity/container/ContainerEntity';
import GlossaryNodeEntity from './app/entity/glossaryNode/GlossaryNodeEntity';

/*
    Construct Apollo Client
*/
const httpLink = createHttpLink({ uri: '/api/v2/graphql' });

const errorLink = onError((error) => {
    const { networkError, graphQLErrors } = error;
    if (networkError) {
        const serverError = networkError as ServerError;
        if (serverError.statusCode === 401) {
            isLoggedInVar(false);
            Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
            window.location.replace(PageRoutes.AUTHENTICATE);
        }
    }
    if (graphQLErrors && graphQLErrors.length) {
        const firstError = graphQLErrors[0];
        const { extensions } = firstError;
        const errorCode = extensions && (extensions.code as number);
        // Fallback in case the calling component does not handle.
        message.error(`${firstError.message} (code ${errorCode})`, 3);
    }
});

const client = new ApolloClient({
    connectToDevTools: true,
    link: errorLink.concat(httpLink),
    cache: new InMemoryCache(),
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

const App: React.VFC = () => {
    const [dynamicThemeConfig, setDynamicThemeConfig] = useState<Theme>(defaultThemeConfig);

    useEffect(() => {
        import(`./conf/theme/${process.env.REACT_APP_THEME_CONFIG}`).then((theme) => {
            setDynamicThemeConfig(theme);
        });
    }, []);

    const entityRegistry = useMemo(() => {
        const register = new EntityRegistry();
        register.register(new DatasetEntity());
        register.register(new DashboardEntity());
        register.register(new ChartEntity());
        register.register(new UserEntity());
        register.register(new GroupEntity());
        register.register(new TagEntity());
        register.register(new DataFlowEntity());
        register.register(new DataJobEntity());
        register.register(new GlossaryTermEntity());
        register.register(new MLFeatureEntity());
        register.register(new MLPrimaryKeyEntity());
        register.register(new MLFeatureTableEntity());
        register.register(new MLModelEntity());
        register.register(new MLModelGroupEntity());
        register.register(new DomainEntity());
        register.register(new ContainerEntity());
        register.register(new GlossaryNodeEntity());
        return register;
    }, []);

    return (
        <ThemeProvider theme={dynamicThemeConfig}>
            <Router>
                <EntityRegistryContext.Provider value={entityRegistry}>
                    <ApolloProvider client={client}>
                        <Routes />
                    </ApolloProvider>
                </EntityRegistryContext.Provider>
            </Router>
        </ThemeProvider>
    );
};

export default App;
