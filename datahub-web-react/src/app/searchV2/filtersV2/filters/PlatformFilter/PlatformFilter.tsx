import { Entity, EntityType } from '@src/types.generated';
import React, { useCallback } from 'react';
import { FilterComponentProps } from '../../types';
import BaseEntityFilter from '../BaseEntityFilter/BaseEntityFilter';
import { EntityIconWithName } from '../BaseEntityFilter/components/EntityIconWithName';

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
