<<<<<<< HEAD
import React, { useCallback } from 'react';

import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import { EntityIconWithName } from '@app/searchV2/filtersV2/filters/BaseEntityFilter/components/EntityIconWithName';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { Entity, EntityType } from '@src/types.generated';
=======
import { Entity, EntityType } from '@src/types.generated';
import React, { useCallback } from 'react';
import { FilterComponentProps } from '../../types';
import BaseEntityFilter from '../BaseEntityFilter/BaseEntityFilter';
import { EntityIconWithName } from '../BaseEntityFilter/components/EntityIconWithName';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export default function PlatformEntityFilter(props: FilterComponentProps) {
    const renderEntity = useCallback((entity: Entity) => <EntityIconWithName entity={entity} />, []);

    return (
        <BaseEntityFilter
            {...props}
            renderEntity={renderEntity}
            entityTypes={[EntityType.DataPlatform]}
            filterName="Platforms"
        />
    );
}
