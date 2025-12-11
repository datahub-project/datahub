/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useCallback } from 'react';

import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import { EntityIconWithName } from '@app/searchV2/filtersV2/filters/BaseEntityFilter/components/EntityIconWithName';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { Entity, EntityType } from '@src/types.generated';

export default function PlatformEntityFilter(props: FilterComponentProps) {
    const renderEntity = useCallback((entity: Entity) => <EntityIconWithName entity={entity} />, []);

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
