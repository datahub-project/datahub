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
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { isTag } from '@src/app/entityV2/tag/utils';
import TagLink from '@src/app/sharedV2/tags/TagLink';
import { Entity, EntityType } from '@src/types.generated';

interface PlatformLabelProps {
    entity: Entity;
}

function TagLabel({ entity }: PlatformLabelProps) {
    const tag = (isTag(entity) && entity) || undefined;

    if (tag === undefined) return null;

    return <TagLink tag={tag} enableTooltip={false} enableDrawer={false} />;
}

export default function TagFilter(props: FilterComponentProps) {
    const renderEntity = useCallback((entity: Entity) => <TagLabel entity={entity} />, []);

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
