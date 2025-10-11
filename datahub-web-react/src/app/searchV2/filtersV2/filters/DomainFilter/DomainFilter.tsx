import React, { useCallback } from 'react';

import { isDomain } from '@app/entityV2/domain/utils';
import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import { Entity, EntityType } from '@src/types.generated';

interface PlatformLabelProps {
    entity: Entity;
}

function DomainLabel({ entity }: PlatformLabelProps) {
    const domain = (isDomain(entity) && entity) || undefined;

    if (domain === undefined) return null;

    return <DomainLink domain={domain} enableTooltip={false} readOnly />;
}

export default function DomainFilter(props: FilterComponentProps) {
    const renderEntity = useCallback((entity: Entity) => <DomainLabel entity={entity} />, []);

    return (
        <BaseEntityFilter
            {...props}
            renderEntity={renderEntity}
            entityTypes={[EntityType.Domain]}
            filterName="Domains"
            dataTestId="filter-domain"
        />
    );
}
