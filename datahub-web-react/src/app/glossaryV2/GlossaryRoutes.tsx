import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Switch, Route, Redirect } from 'react-router-dom';
import { PageRoutes } from '../../conf/Global';
import { GlossaryEntityContext } from '../entityV2/shared/GlossaryEntityContext';
import { GenericEntityProperties } from '../entityV2/shared/types';
import BusinessGlossaryPage from './BusinessGlossaryPage';
import GlossaryEntitiesPath from './GlossaryEntitiesPath';
import { EntityPage as EntityPageV2 } from '../entityV2/EntityPage';
import GlossarySidebar from './GlossarySidebar';
import { useEntityRegistry } from '../useEntityRegistry';
import { useAppConfig } from '../useAppConfig';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { shouldShowGlossary } from '../identity/user/UserUtils';

const ContentWrapper = styled.div`
    display: flex;
    flex: 1;
    overflow: hidden;
`;

export default function GlossaryRoutes() {
    const entityRegistry = useEntityRegistry();
    const [entityData, setEntityData] = useState<GenericEntityProperties | null>(null);
    const [urnsToUpdate, setUrnsToUpdate] = useState<string[]>([]);
    const [isSidebarOpen, setIsSidebarOpen] = useState<boolean>(true);

    const isAtRootGlossary = window.location.pathname === PageRoutes.GLOSSARY;
    const appConfig = useAppConfig();
    const authenticatedUser = useGetAuthenticatedUser();
    const canManageGlossary = authenticatedUser?.platformPrivileges.manageGlossaries || false;
    const hideGlossary = !!appConfig?.config?.visualConfig?.hideGlossary;
    const showGlossary = shouldShowGlossary(canManageGlossary, hideGlossary);

    return (
        <GlossaryEntityContext.Provider
            value={{
                isInGlossaryContext: true,
                entityData,
                setEntityData,
                urnsToUpdate,
                setUrnsToUpdate,
                isSidebarOpen,
                setIsSidebarOpen,
            }}
        >
            <ContentWrapper>
                <GlossarySidebar />
                <Switch>
                    {entityRegistry.getGlossaryEntities().map((entity) => (
                        <Route
                            key={entity.getPathName()}
                            path={`/${entity.getPathName()}/:urn`}
                            render={() => <EntityPageV2 entityType={entity.type} />}
                        />
                    ))}
                    <Route
                        path={PageRoutes.GLOSSARY}
                        render={() => (showGlossary ? <BusinessGlossaryPage /> : <Redirect to="/" />)}
                    />
                </Switch>
            </ContentWrapper>
        </GlossaryEntityContext.Provider>
    );
}
