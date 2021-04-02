import React from 'react';
import { useParams } from 'react-router-dom';
import { EntityType } from '../../types.generated';
import { BrowsableEntityPage } from '../browse/BrowsableEntityPage';
import LineageExplorer from '../lineage/LineageExplorer';
import useIsLineageMode from '../lineage/utils/useIsLineageMode';
import { SearchablePage } from '../search/SearchablePage';
import { useEntityRegistry } from '../useEntityRegistry';

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
    const { urn } = useParams<RouteParams>();
    const entityRegistry = useEntityRegistry();
    const isBrowsable = entityRegistry.getEntity(entityType).isBrowseEnabled();
    const ContainerPage = isBrowsable ? BrowsableEntityPage : SearchablePage;
    const isLineageMode = useIsLineageMode();

    // TODO(gabe-lyons): pull this logic into the entity registry
    const isLineageSupported = entityType === EntityType.Dataset;

    return (
        <ContainerPage urn={urn} type={entityType} lineageSupported={isLineageSupported}>
            {isLineageMode && isLineageSupported ? <LineageExplorer /> : entityRegistry.renderProfile(entityType, urn)}
        </ContainerPage>
    );
};
