import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Switch, Route, Redirect } from 'react-router-dom';
import { PageRoutes } from '../../conf/Global';
import { GlossaryEntityContext } from '../entity/shared/GlossaryEntityContext';
import { GenericEntityProperties } from '../entity/shared/types';
import BusinessGlossaryPage from './BusinessGlossaryPage';
import BusinessGlossaryPageV2 from '../glossaryV2/BusinessGlossaryPage';
import { EntityPage } from '../entity/EntityPage';
import GlossarySidebar from './GlossarySidebar';
import { useEntityRegistry } from '../useEntityRegistry';
import { useAppConfig } from '../useAppConfig';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { shouldShowGlossary } from '../identity/user/UserUtils';
import { useIsThemeV2 } from '../useIsThemeV2';

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

    const appConfig = useAppConfig();
    const authenticatedUser = useGetAuthenticatedUser();
    const isThemeV2 = useIsThemeV2();
    const canManageGlossary = authenticatedUser?.platformPrivileges?.manageGlossaries || false;
    const hideGlossary = !!appConfig?.config?.visualConfig?.hideGlossary;
    const showGlossary = shouldShowGlossary(canManageGlossary, hideGlossary);

    const renderPage = (type1: boolean, type2: boolean) => {
        if (type1) {
            if (type2) {
                return <BusinessGlossaryPageV2 />;
            }
            return <Redirect to="/" />;
        }
        if (type2) {
            return <BusinessGlossaryPage />;
        }
        return <Redirect to="/" />;
    };

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
                            render={() => <EntityPage entityType={entity.type} />}
                        />
                    ))}
                    <Route path={PageRoutes.GLOSSARY} render={() => renderPage(isThemeV2, showGlossary)} />
                </Switch>
            </ContentWrapper>
        </GlossaryEntityContext.Provider>
    );
}
