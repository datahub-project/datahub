import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Switch, Route, Redirect } from 'react-router-dom';
import { PageRoutes } from '../../conf/Global';
import { GlossaryEntityContext } from '../entityV2/shared/GlossaryEntityContext';
import { GenericEntityProperties } from '../entity/shared/types';
import BusinessGlossaryPage from './BusinessGlossaryPage';
import { EntityPage as EntityPageV2 } from '../entityV2/EntityPage';
import GlossarySidebar from './GlossarySidebar';
import { useEntityRegistry } from '../useEntityRegistry';
import { useAppConfig } from '../useAppConfig';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { shouldShowGlossary } from '../identity/user/UserUtils';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

const ContentWrapper = styled.div<{ isShowNavBarRedesign?: boolean }>`
    display: flex;
    flex: 1;
    overflow: hidden;
    gap: ${(props) => (props.isShowNavBarRedesign ? '16px' : '0')};
`;

export default function GlossaryRoutes() {
    const entityRegistry = useEntityRegistry();
    const [entityData, setEntityData] = useState<GenericEntityProperties | null>(null);
    const [urnsToUpdate, setUrnsToUpdate] = useState<string[]>([]);
    const [isSidebarOpen, setIsSidebarOpen] = useState<boolean>(true);

    const appConfig = useAppConfig();
    const authenticatedUser = useGetAuthenticatedUser();
    const canManageGlossary = authenticatedUser?.platformPrivileges.manageGlossaries || false;
    const hideGlossary = !!appConfig?.config?.visualConfig?.hideGlossary;
    const showGlossary = shouldShowGlossary(canManageGlossary, hideGlossary);
    const isShowNavBarRedesign = useShowNavBarRedesign();

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
            <ContentWrapper isShowNavBarRedesign={isShowNavBarRedesign}>
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
