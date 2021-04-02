import React from 'react';
import { useParams } from 'react-router-dom';
import { EntityType } from '../../types.generated';
import { BrowsableEntityPage } from '../browse/BrowsableEntityPage';
import { SearchablePage } from '../search/SearchablePage';
import { useEntityRegistry } from '../useEntityRegistry';

interface RouteParams {
    urn: string;
}

interface Props {
    overrideUrn?: string;
    entityType: EntityType;
}

/**
 * Responsible for rendering an Entity Profile
 */
export const EntityPage = ({ overrideUrn, entityType }: Props) => {
    const { urn } = useParams<RouteParams>();
    const entityRegistry = useEntityRegistry();
    const isBrowsable = entityRegistry.getEntity(entityType).isBrowseEnabled();
    const ContainerPage = isBrowsable ? BrowsableEntityPage : SearchablePage;
    return (
        <ContainerPage urn={overrideUrn || urn} type={entityType} lineageEnabled={entityType === EntityType.Dataset}>
            {entityRegistry.renderProfile(entityType, overrideUrn || urn)}
        </ContainerPage>
    );
};
