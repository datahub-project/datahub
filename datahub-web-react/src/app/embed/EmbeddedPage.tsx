import React, { useEffect } from 'react';
import { useParams } from 'react-router';
import styled from 'styled-components/macro';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { UnauthorizedPage } from '@app/authorization/UnauthorizedPage';
import { useUserContext } from '@app/context/useUserContext';
import { VIEW_ENTITY_PAGE } from '@app/entity/shared/constants';
import { decodeUrn } from '@app/entity/shared/utils';
import CompactContext from '@app/shared/CompactContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetGrantedPrivilegesQuery } from '@graphql/policy.generated';
import { EntityType } from '@types';

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
