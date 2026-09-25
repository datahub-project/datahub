import React, { useState } from 'react';
import { Redirect, Route, Switch } from 'react-router-dom';

import { useUserContext } from '@app/context/useUserContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { EntityPage as EntityPageV2 } from '@app/entityV2/EntityPage';
import { GlossaryEntityContext } from '@app/entityV2/shared/GlossaryEntityContext';
import BusinessGlossaryPage from '@app/glossaryV2/BusinessGlossaryPage';
import GlossarySidebar from '@app/glossaryV2/GlossarySidebar';
import { shouldShowGlossary } from '@app/identity/user/UserUtils';
import {
    HierarchicalBrowseContentWrapper,
    HierarchicalBrowseMainContent,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseLayout.components';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';
import { Entity } from '@src/types.generated';

export default function GlossaryRoutes() {
    const entityRegistry = useEntityRegistry();
    const [entityData, setEntityData] = useState<GenericEntityProperties | null>(null);
    const [urnsToUpdate, setUrnsToUpdate] = useState<string[]>([]);
    const [isSidebarOpen, setIsSidebarOpen] = useState<boolean>(true);
    const [nodeToNewEntity, setNodeToNewEntity] = useState<Record<string, Entity>>({});
    const [nodeToDeletedUrn, setNodeToDeletedUrn] = useState<Record<string, string>>({});

    const appConfig = useAppConfig();
    const { platformPrivileges } = useUserContext();
    const canManageGlossary = platformPrivileges?.manageGlossaries || false;
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
                nodeToNewEntity,
                setNodeToNewEntity,
                nodeToDeletedUrn,
                setNodeToDeletedUrn,
            }}
        >
            <HierarchicalBrowseContentWrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
                <GlossarySidebar />
                <HierarchicalBrowseMainContent>
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
                </HierarchicalBrowseMainContent>
            </HierarchicalBrowseContentWrapper>
        </GlossaryEntityContext.Provider>
    );
}
