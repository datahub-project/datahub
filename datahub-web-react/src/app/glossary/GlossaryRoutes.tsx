import React, { useState } from 'react';
import { Redirect, Route, Switch } from 'react-router-dom';
import styled from 'styled-components/macro';

import { EntityPage } from '@app/entity/EntityPage';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { GlossaryEntityContext } from '@app/entityV2/shared/GlossaryEntityContext';
import GlossarySidebar from '@app/glossary/GlossarySidebar';
import BusinessGlossaryPageV2 from '@app/glossaryV2/BusinessGlossaryPage';
import { shouldShowGlossary } from '@app/identity/user/UserUtils';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useGetAuthenticatedUser } from '@app/useGetAuthenticatedUser';
import { PageRoutes } from '@conf/Global';
import { Entity } from '@src/types.generated';

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
    const [nodeToNewEntity, setNodeToNewEntity] = useState<Record<string, Entity>>({});
    const [nodeToDeletedUrn, setNodeToDeletedUrn] = useState<Record<string, string>>({});

    const appConfig = useAppConfig();
    const authenticatedUser = useGetAuthenticatedUser();
    const canManageGlossary = authenticatedUser?.platformPrivileges?.manageGlossaries || false;
    const hideGlossary = !!appConfig?.config?.visualConfig?.hideGlossary;
    const showGlossary = shouldShowGlossary(canManageGlossary, hideGlossary);

    const renderPage = (isGlossaryAvailable: boolean) => {
        if (isGlossaryAvailable) {
            return <BusinessGlossaryPageV2 />;
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
                nodeToNewEntity,
                setNodeToNewEntity,
                nodeToDeletedUrn,
                setNodeToDeletedUrn,
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
                    <Route path={PageRoutes.GLOSSARY} render={() => renderPage(showGlossary)} />
                </Switch>
            </ContentWrapper>
        </GlossaryEntityContext.Provider>
    );
}
