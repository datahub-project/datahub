import React, { useEffect } from 'react';
import { useParams } from 'react-router';
import styled from 'styled-components/macro';
import { useGetGrantedPrivilegesQuery } from '../../graphql/policy.generated';
import { EntityType } from '../../types.generated';
import { UnauthorizedPage } from '../authorization/UnauthorizedPage';
import { VIEW_ENTITY_PAGE } from '../entity/shared/constants';
import { decodeUrn } from '../entity/shared/utils';
import CompactContext from '../shared/CompactContext';
import { useEntityRegistry } from '../useEntityRegistry';
import analytics from '../analytics/analytics';
import { EventType } from '../analytics';
import { useUserContext } from '../context/useUserContext';

const EmbeddedPageWrapper = styled.div`
    max-height: 100%;
    overflow: auto;
    padding: 16px;
`;

interface RouteParams {
    urn: string;
}

interface Props {
    entityType: EntityType;
}

export default function EmbeddedPage({ entityType }: Props) {
    const entityRegistry = useEntityRegistry();
    const { urn: encodedUrn } = useParams<RouteParams>();
    const urn = decodeUrn(encodedUrn);

    useEffect(() => {
        analytics.event({
            type: EventType.EmbedProfileViewEvent,
            entityType,
            entityUrn: urn,
        });
    }, [entityType, urn]);

    const { urn: authenticatedUserUrn } = useUserContext();
    const { data } = useGetGrantedPrivilegesQuery({
        variables: {
            input: {
                actorUrn: authenticatedUserUrn as string,
                resourceSpec: { resourceType: entityType, resourceUrn: urn },
            },
        },
        fetchPolicy: 'cache-first',
        skip: !authenticatedUserUrn,
    });

    const privileges = data?.getGrantedPrivileges?.privileges || [];
    const canViewEntityPage = privileges.find((privilege) => privilege === VIEW_ENTITY_PAGE);

    return (
        <CompactContext.Provider value>
            {data && !canViewEntityPage && <UnauthorizedPage />}
            {data && canViewEntityPage && (
                <EmbeddedPageWrapper>{entityRegistry.renderEmbeddedProfile(entityType, urn)}</EmbeddedPageWrapper>
            )}
        </CompactContext.Provider>
    );
}
