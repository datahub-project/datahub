import React, { useEffect } from 'react';
import { useParams } from 'react-router-dom';

import analytics, { EventType } from '@app/analytics';
import { UnauthorizedPage } from '@app/authorization/UnauthorizedPage';
import { BrowsableEntityPage } from '@app/browse/BrowsableEntityPage';
import { useUserContext } from '@app/context/useUserContext';
import { VIEW_ENTITY_PAGE } from '@app/entity/shared/constants';
import { decodeUrn } from '@app/entity/shared/utils';
import LineageExplorer from '@app/lineage/LineageExplorer';
import useIsLineageMode from '@app/lineage/utils/useIsLineageMode';
import { ErrorSection } from '@app/shared/error/ErrorSection';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetGrantedPrivilegesQuery } from '@graphql/policy.generated';
import { EntityType } from '@types';

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
                resourceSpec: { resourceType: entityType, resourceUrn: urn },
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
        entityType === EntityType.DataProcessInstance ||
        entityType === EntityType.GlossaryNode;

    return (
        <>
            {error && <ErrorSection />}
            {data && !canViewEntityPage && <UnauthorizedPage />}
            {canViewEntityPage &&
                ((showNewPage && <>{entityRegistry.renderProfile(entityType, urn)}</>) || (
                    <BrowsableEntityPage
                        isBrowsable={isBrowsable}
                        urn={urn}
                        type={entityType}
                        lineageSupported={isLineageSupported}
                    >
                        {isLineageMode && isLineageSupported ? (
                            <LineageExplorer type={entityType} urn={urn} />
                        ) : (
                            entityRegistry.renderProfile(entityType, urn)
                        )}
                    </BrowsableEntityPage>
                ))}
        </>
    );
};
