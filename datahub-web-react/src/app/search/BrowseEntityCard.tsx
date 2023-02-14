import React from 'react';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../useEntityRegistry';
import { PageRoutes } from '../../conf/Global';
import { IconStyleType } from '../entity/Entity';
import { EntityType } from '../../types.generated';
import { LogoCountCard } from '../shared/LogoCountCard';
import { EventType } from '../analytics/event';
import analytics from '../analytics';

export const BrowseEntityCard = ({ entityType, count }: { entityType: EntityType; count: number }) => {
    const entityRegistry = useEntityRegistry();
    const isGlossaryEntityCard = entityType === EntityType.GlossaryTerm;
    const entityPathName = entityRegistry.getPathName(entityType);
    const url = isGlossaryEntityCard ? PageRoutes.GLOSSARY : `${PageRoutes.BROWSE}/${entityPathName}`;
    const onBrowseEntityCardClick = () => {
        analytics.event({
            type: EventType.HomePageBrowseResultClickEvent,
            entityType,
        });
    };

    return (
        <Link to={url} data-testid={`entity-type-browse-card-${entityType}`}>
            <LogoCountCard
                logoComponent={entityRegistry.getIcon(entityType, 18, IconStyleType.HIGHLIGHT)}
                name={entityRegistry.getCollectionName(entityType)}
                count={count}
                onClick={onBrowseEntityCardClick}
            />
        </Link>
    );
};
