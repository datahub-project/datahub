import React from 'react';
import { useSearchForEntityUrnsByFormQuery } from '../../../../../graphql/form.generated';
import { useEntityContext } from '../../EntityContext';
import { useEntityFormContext } from '../EntityFormContext';
import { Entity, FormFilter } from '../../../../../types.generated';
import { ArrowLeft, ArrowRight, BulkNavigationWrapper, NavigationWrapper } from './components';

export default function EntityNavigation() {
    const { formFilter } = useEntityFormContext();
    const { selectedEntity, setSelectedEntity } = useEntityFormContext();
    const { entityData } = useEntityContext();
    const { data } = useSearchForEntityUrnsByFormQuery({
        variables: {
            input: {
                formFilter: formFilter as FormFilter,
                start: 0,
                count: 1000,
            },
        },
    });
    // place current entity first in array no matter what
    const entitiesForForm = data?.searchForEntitiesByForm?.searchResults
        .map((result) => result.entity)
        .filter((result) => result.urn !== entityData?.urn);
    entitiesForForm?.unshift(entityData as Entity);
    const currentEntityIndex = entitiesForForm?.findIndex((entity) => entity.urn === selectedEntity?.urn) || 0;

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
            <NavigationWrapper isHidden={!data}>
                <ArrowLeft onClick={navigateLeft} />
                {currentEntityIndex + 1}
                &nbsp;of&nbsp;
                {entitiesForForm?.length}
                &nbsp;Entities
                <ArrowRight onClick={navigateRight} />
            </NavigationWrapper>
        </BulkNavigationWrapper>
    );
}
