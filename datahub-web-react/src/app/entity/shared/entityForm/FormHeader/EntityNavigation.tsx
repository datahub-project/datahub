import React from 'react';

import { pluralize } from '@src/app/shared/textUtil';
import analytics, { EventType } from '@src/app/analytics';
import { useEntityFormContext } from '../EntityFormContext';
import { Entity } from '../../../../../types.generated';
import { ArrowLeft, ArrowRight, BulkNavigationWrapper, NavigationWrapper } from './components';
import { MAX_ENTITY_URN_COUNT } from '../entityFormDataFactory';

export default function EntityNavigation() {
    const {
        entity: { selectedEntity, setSelectedEntity, entitiesForForm },
    } = useEntityFormContext();

    if (!selectedEntity) return null;

    const currentEntityIndex =
        entitiesForForm?.findIndex((entity) => selectedEntity && entity.urn === selectedEntity?.urn) || 0;

    function navigateLeft() {
        analytics.event({ type: EventType.FormByEntityNavigate, direction: 'backward' });
        if (currentEntityIndex === 0) {
            setSelectedEntity(entitiesForForm?.[(entitiesForForm?.length || 0) - 1] as Entity);
        } else {
            setSelectedEntity(entitiesForForm?.[currentEntityIndex - 1] as Entity);
        }
    }

    function navigateRight() {
        analytics.event({ type: EventType.FormByEntityNavigate, direction: 'forward' });
        if (currentEntityIndex === (entitiesForForm?.length || 0) - 1) {
            setSelectedEntity(entitiesForForm?.[0] as Entity);
        } else {
            setSelectedEntity(entitiesForForm?.[currentEntityIndex + 1] as Entity);
        }
    }

    const entityCount = entitiesForForm?.length;

    return (
        <BulkNavigationWrapper>
            <NavigationWrapper isHidden={!entitiesForForm}>
                <ArrowLeft onClick={navigateLeft} />
                {currentEntityIndex + 1}
                &nbsp;of&nbsp;
                {(entityCount || 0) >= MAX_ENTITY_URN_COUNT ? `${MAX_ENTITY_URN_COUNT.toLocaleString()}+` : entityCount}
                &nbsp;{pluralize(entityCount || 0, 'Asset')}
                <ArrowRight onClick={navigateRight} />
            </NavigationWrapper>
        </BulkNavigationWrapper>
    );
}
