import React from 'react';

import { useEntityFormContext } from '../EntityFormContext';
import { Entity } from '../../../../../types.generated';
import { ArrowLeft, ArrowRight, BulkNavigationWrapper, NavigationWrapper } from './components';

export default function EntityNavigation() {
    const {
        entity: { selectedEntity, setSelectedEntity, entitiesForForm },
    } = useEntityFormContext();

    if (!selectedEntity) return null;

    const currentEntityIndex =
        entitiesForForm?.findIndex((entity) => selectedEntity && entity.urn === selectedEntity?.urn) || 0;

    function navigateLeft() {
        if (currentEntityIndex === 0) {
            setSelectedEntity(entitiesForForm?.[(entitiesForForm?.length || 0) - 1] as Entity);
        } else {
            setSelectedEntity(entitiesForForm?.[currentEntityIndex - 1] as Entity);
        }
    }

    function navigateRight() {
        if (currentEntityIndex === (entitiesForForm?.length || 0) - 1) {
            setSelectedEntity(entitiesForForm?.[0] as Entity);
        } else {
            setSelectedEntity(entitiesForForm?.[currentEntityIndex + 1] as Entity);
        }
    }

    return (
        <BulkNavigationWrapper>
            <NavigationWrapper isHidden={!entitiesForForm}>
                <ArrowLeft onClick={navigateLeft} />
                {currentEntityIndex + 1}
                &nbsp;of&nbsp;
                {entitiesForForm?.length}
                &nbsp;Assets
                <ArrowRight onClick={navigateRight} />
            </NavigationWrapper>
        </BulkNavigationWrapper>
    );
}
