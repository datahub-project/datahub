import React, { useEffect } from 'react';
import { Alert } from 'antd';
import { useParams } from 'react-router-dom';
import { EntityType } from '../../types.generated';
import { BrowsableEntityPage } from '../browse/BrowsableEntityPage';
import LineageExplorer from '../lineage/LineageExplorer';
import useIsLineageMode from '../lineage/utils/useIsLineageMode';
import { SearchablePage } from '../search/SearchablePage';
import { useEntityRegistry } from '../useEntityRegistry';
import analytics, { EventType } from '../analytics';
import { decodeUrn } from './shared/utils';
import { useGetAuthenticatedUserUrn } from '../useGetAuthenticatedUser';
import { useGetGrantedPrivilegesQuery } from '../../graphql/policy.generated';
import { Message } from '../shared/Message';
import { UnauthorizedPage } from '../authorization/UnauthorizedPage';

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
    const ContainerPage = isBrowsable || isLineageSupported ? BrowsableEntityPage : SearchablePage;
    const isLineageMode = useIsLineageMode();
    const authenticatedUserUrn = useGetAuthenticatedUserUrn();
    const { loading, error, data } = useGetGrantedPrivilegesQuery({
        variables: {
            input: {
                actorUrn: authenticatedUserUrn,
                resourceSpec: { resourceType: entityType, resourceUrn: urn },
            },
        },
    });
    const privileges = data?.getGrantedPrivileges?.privileges || [];

    useEffect(() => {
        analytics.event({
            type: EventType.EntityViewEvent,
            entityType,
            entityUrn: urn,
        });
    }, [entityType, urn]);

    const canViewEntityPage = privileges.find((privilege) => privilege === 'VIEW_ENTITY_PAGE');
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
        entityType === EntityType.GlossaryTerm;

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {error && <Alert type="error" message={error?.message || `Failed to fetch privileges for user`} />}
            {data && !canViewEntityPage && <UnauthorizedPage />}
            {canViewEntityPage &&
                ((showNewPage && <SearchablePage>{entityRegistry.renderProfile(entityType, urn)}</SearchablePage>) || (
                    <ContainerPage
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
                    </ContainerPage>
                ))}
        </>
    );
};
