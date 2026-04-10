import React, { useCallback } from 'react';

import { DefaultEntitySelectOption } from '@app/entityV2/shared/components/select/DefaultEntitySelectOption';
import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { Entity, EntityType } from '@src/types.generated';

export default function PlatformEntityFilter(props: FilterComponentProps) {
    const renderEntity = useCallback((entity: Entity) => <DefaultEntitySelectOption entity={entity} />, []);

    return (
        <BaseEntityFilter
            {...props}
            renderEntity={renderEntity}
            entityTypes={[EntityType.DataPlatform]}
            filterName="Platforms"
            dataTestId="filter-platform"
        />
    );
}
