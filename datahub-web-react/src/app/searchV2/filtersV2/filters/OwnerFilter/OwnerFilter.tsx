import React, { useCallback } from 'react';

import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { OwnerSelectOption } from '@app/sharedV2/owners/OwnerSelectOption';
import { Entity, EntityType } from '@src/types.generated';

export default function OwnerFilter(props: FilterComponentProps) {
    const renderEntity = useCallback((entity: Entity) => <OwnerSelectOption entity={entity} />, []);

    return (
        <BaseEntityFilter
            {...props}
            renderEntity={renderEntity}
            entityTypes={[EntityType.CorpUser, EntityType.CorpGroup]}
            filterName="Owner"
            dataTestId="filter-owner"
        />
    );
}
