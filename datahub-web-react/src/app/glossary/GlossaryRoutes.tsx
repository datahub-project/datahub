import React, { useState } from 'react';
import { Redirect, Route, Switch } from 'react-router-dom';
import styled from 'styled-components/macro';

import { EntityPage } from '@app/entity/EntityPage';
import { GlossaryEntityContext } from '@app/entity/shared/GlossaryEntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import BusinessGlossaryPage from '@app/glossary/BusinessGlossaryPage';
import GlossarySidebar from '@app/glossary/GlossarySidebar';
import BusinessGlossaryPageV2 from '@app/glossaryV2/BusinessGlossaryPage';
import { shouldShowGlossary } from '@app/identity/user/UserUtils';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useGetAuthenticatedUser } from '@app/useGetAuthenticatedUser';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { PageRoutes } from '@conf/Global';

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
