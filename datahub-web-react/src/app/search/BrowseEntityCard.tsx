import React from 'react';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../useEntityRegistry';
import { PageRoutes } from '../../conf/Global';
import { IconStyleType } from '../entity/Entity';
import { EntityType } from '../../types.generated';
import { LogoCountCard } from '../shared/LogoCountCard';

export const BrowseEntityCard = ({ entityType, count }: { entityType: EntityType; count: number }) => {
    const entityRegistry = useEntityRegistry();
    return (
        <Link
            to={`${PageRoutes.BROWSE}/${entityRegistry.getPathName(entityType)}`}
            data-testid={`entity-type-browse-card-${entityType}`}
        >
            <LogoCountCard
                logoComponent={entityRegistry.getIcon(entityType, 18, IconStyleType.HIGHLIGHT)}
                name={entityRegistry.getCollectionName(entityType)}
                count={count}
            />
        </Link>
    );
};
