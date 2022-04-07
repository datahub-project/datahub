import React from 'react';
import { Link } from 'react-router-dom';
import { PageRoutes } from '../../../conf/Global';
import { EntityType } from '../../../types.generated';
import { IconStyleType } from '../../entity/Entity';
import { LogoCountCard } from '../../shared/LogoCountCard';
import { useEntityRegistry } from '../../useEntityRegistry';

export const ContainerBrowseEntityCard = ({ entityType, count }: { entityType: EntityType; count: number }) => {
    const entityRegistry = useEntityRegistry();
    if (entityType === EntityType.GlossaryTerm) {
        return (
            <Link to={`${PageRoutes.BROWSE}/${entityRegistry.getPathName(entityType)}`}>
                <LogoCountCard
                    logoComponent={entityRegistry.getIcon(entityType, 18, IconStyleType.HIGHLIGHT)}
                    name={entityRegistry.getCollectionName(entityType)}
                    count={count}
                />
            </Link>
        );
    }
    return (
        <Link to={`${PageRoutes.METADATA}/${entityRegistry.getPathName(entityType)}`}>
            <LogoCountCard
                logoComponent={entityRegistry.getIcon(entityType, 18, IconStyleType.HIGHLIGHT)}
                name={entityRegistry.getCollectionName(entityType)}
                count={count}
            />
        </Link>
    );
};
