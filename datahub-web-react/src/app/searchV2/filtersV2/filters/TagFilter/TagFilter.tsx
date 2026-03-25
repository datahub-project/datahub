import React, { useCallback } from 'react';

import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { TagSelectOption } from '@app/sharedV2/tags/TagSelectOption';
import { Entity, EntityType } from '@src/types.generated';

export default function TagFilter(props: FilterComponentProps) {
    const renderEntity = useCallback((entity: Entity) => <TagSelectOption entity={entity} />, []);

    return (
        <BaseEntityFilter
            {...props}
            renderEntity={renderEntity}
            entityTypes={[EntityType.Tag]}
            filterName="Tags"
            dataTestId="filter-tag"
        />
    );
}
