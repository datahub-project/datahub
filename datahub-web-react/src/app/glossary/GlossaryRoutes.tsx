import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Switch, Route } from 'react-router-dom';
import { PageRoutes } from '../../conf/Global';
import { GlossaryEntityContext } from '../entity/shared/GlossaryEntityContext';
import { GenericEntityProperties } from '../entity/shared/types';
import BusinessGlossaryPage from './BusinessGlossaryPage';
import GlossaryEntitiesPath from './GlossaryEntitiesPath';
import { EntityPage } from '../entity/EntityPage';
import GlossarySidebar from './GlossarySidebar';
import { useEntityRegistry } from '../useEntityRegistry';

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
            {!isAtRootGlossary && <GlossaryEntitiesPath />}
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
                    <Route path={PageRoutes.GLOSSARY} render={() => <BusinessGlossaryPage />} />
                </Switch>
            </ContentWrapper>
        </GlossaryEntityContext.Provider>
    );
}
