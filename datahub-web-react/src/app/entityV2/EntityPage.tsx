import React, { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';

import analytics, { EventType } from '@app/analytics';
import { UnauthorizedPage } from '@app/authorization/UnauthorizedPage';
import { BrowsableEntityPage } from '@app/browse/BrowsableEntityPage';
import { useUserContext } from '@app/context/useUserContext';
import { VIEW_ENTITY_PAGE } from '@app/entityV2/shared/constants';
import { decodeUrn } from '@app/entityV2/shared/utils';
import LineageExplorer from '@app/lineage/LineageExplorer';
import useIsLineageMode from '@app/lineage/utils/useIsLineageMode';
import { useLineageV2 } from '@app/lineageV2/useLineageV2';
import TabFullSizedContext from '@app/shared/TabFullsizedContext';
import { ErrorSection } from '@app/shared/error/ErrorSection';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetGrantedPrivilegesQuery } from '@graphql/policy.generated';
import { EntityType } from '@types';

interface RouteParams {
    urn: string;
}

interface Props {
    entityType: EntityType;
}

const ALLOWED_ENTITY_TYPES = [
    EntityType.Dataset,
    EntityType.Dashboard,
    EntityType.Chart,
    EntityType.DataFlow,
    EntityType.DataJob,
    EntityType.Mlmodel,
    EntityType.Mlfeature,
    EntityType.MlprimaryKey,
    EntityType.MlfeatureTable,
    EntityType.MlmodelGroup,
    EntityType.GlossaryTerm,
    EntityType.GlossaryNode,
    EntityType.SchemaField,
];

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
    const showNewPage = ALLOWED_ENTITY_TYPES.includes(entityType);

    const isLineageV2 = useLineageV2();
    const showLineage = isLineageMode && isLineageSupported;
    const [isSidebarClosed, setIsSidebarClosed] = useState(false);
    const [isTabFullsize, setTabFullsize] = useState(false);
    const sidebarWidth = useSidebarWidth();

    return (
        <>
            {error && <ErrorSection />}
            {data && !canViewEntityPage && <UnauthorizedPage />}
            {canViewEntityPage && (
                <EntitySidebarContext.Provider
                    value={{
                        width: sidebarWidth,
                        isClosed: isSidebarClosed,
                        setSidebarClosed: setIsSidebarClosed,
                    }}
                >
                    <TabFullSizedContext.Provider
                        value={{
                            isTabFullsize,
                            // TODO: Clean up logic after removing lineageGraphV2 flag
                            setTabFullsize: isLineageV2 && showLineage ? undefined : setTabFullsize,
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
                                {(!showLineage || isLineageV2) && entityRegistry.renderProfile(entityType, urn)}
                            </BrowsableEntityPage>
                        )}
                    </TabFullSizedContext.Provider>
                </EntitySidebarContext.Provider>
            )}
        </>
    );
};
