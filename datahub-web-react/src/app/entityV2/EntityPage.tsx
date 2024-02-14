import React, { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { EntityType } from '../../types.generated';
import { BrowsableEntityPage } from '../browse/BrowsableEntityPage';
import LineageExplorer from '../lineage/LineageExplorer';
import LineageExplorerV2 from '../lineageV2/LineageExplorer';
import useIsLineageMode from '../lineage/utils/useIsLineageMode';
import { useLineageV2 } from '../lineageV2/useLineageV2';
import { useEntityRegistry } from '../useEntityRegistry';
import analytics, { EventType } from '../analytics';
import { decodeUrn } from './shared/utils';
import { useGetGrantedPrivilegesQuery } from '../../graphql/policy.generated';
import { UnauthorizedPage } from '../authorization/UnauthorizedPage';
import { ErrorSection } from '../shared/error/ErrorSection';
import { VIEW_ENTITY_PAGE } from './shared/constants';
import { useUserContext } from '../context/useUserContext';
import EntitySidebarContext from '../shared/EntitySidebarContext';
import TabFullSizedContext from '../shared/TabFullsizedContext';

interface RouteParams {
    urn: string;
}

interface Props {
    entityType: EntityType;
}

/**
 * Responsible for rendering an Entity Profile
 */
export const EntityPage = ({ entityType }: Props) => {
    const { urn: encodedUrn } = useParams<RouteParams>();
    const urn = decodeUrn(encodedUrn);
    const entityRegistry = useEntityRegistry();
    const entity = entityRegistry.getEntity(entityType);
    const isBrowsable = entity.isBrowseEnabled();
    const isLineageSupported = entity.isLineageEnabled();
    const isLineageMode = useIsLineageMode();
    const authenticatedUserUrn = useUserContext()?.user?.urn;
    const { error, data } = useGetGrantedPrivilegesQuery({
        variables: {
            input: {
                actorUrn: authenticatedUserUrn as string,
                resourceSpec: {
                    resourceType: entityType,
                    resourceUrn: urn,
                },
            },
        },
        skip: !authenticatedUserUrn,
        fetchPolicy: 'cache-first',
    });
    const privileges = data?.getGrantedPrivileges?.privileges || [];

    useEffect(() => {
        analytics.event({
            type: EventType.EntityViewEvent,
            entityType,
            entityUrn: urn,
        });
    }, [entityType, urn]);

    const canViewEntityPage = privileges.find((privilege) => privilege === VIEW_ENTITY_PAGE);
    const showNewPage =
        entityType === EntityType.Dataset ||
        entityType === EntityType.Dashboard ||
        entityType === EntityType.Chart ||
        entityType === EntityType.DataFlow ||
        entityType === EntityType.DataJob ||
        entityType === EntityType.Mlmodel ||
        entityType === EntityType.Mlfeature ||
        entityType === EntityType.MlprimaryKey ||
        entityType === EntityType.MlfeatureTable ||
        entityType === EntityType.MlmodelGroup ||
        entityType === EntityType.GlossaryTerm ||
        entityType === EntityType.GlossaryNode;

    const isLineageV2 = useLineageV2();
    const showLineage = isLineageMode && isLineageSupported;
    const [isSidebarClosed, setIsSidebarClosed] = useState(false);
    const [isTabFullsize, setTabFullsize] = useState(false);

    return (
        <>
            {error && <ErrorSection />}
            {data && !canViewEntityPage && <UnauthorizedPage />}
            {canViewEntityPage && (
                <EntitySidebarContext.Provider
                    value={{
                        isClosed: isSidebarClosed,
                        setSidebarClosed: setIsSidebarClosed,
                    }}
                >
                    <TabFullSizedContext.Provider
                        value={{
                            isTabFullsize,
                            setTabFullsize,
                        }}
                    >
                        {showNewPage && entityRegistry.renderProfile(entityType, urn)}
                        {!showNewPage && (
                            <BrowsableEntityPage
                                isBrowsable={isBrowsable}
                                urn={urn}
                                type={entityType}
                                lineageSupported={isLineageSupported}
                            >
                                {showLineage && !isLineageV2 && <LineageExplorer type={entityType} urn={urn} />}
                                {showLineage && isLineageV2 && <LineageExplorerV2 urn={urn} type={entityType} />}
                                {!showLineage && entityRegistry.renderProfile(entityType, urn)}
                            </BrowsableEntityPage>
                        )}
                    </TabFullSizedContext.Provider>
                </EntitySidebarContext.Provider>
            )}
        </>
    );
};
