import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';

import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import { EntityIconWithName } from '@app/searchV2/filtersV2/filters/BaseEntityFilter/components/EntityIconWithName';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { Entity, EntityType } from '@src/types.generated';

export default function PlatformEntityFilter(props: FilterComponentProps) {
    const { t } = useTranslation('search');
    const renderEntity = useCallback((entity: Entity) => <EntityIconWithName entity={entity} />, []);

    return (
        <BaseEntityFilter
            {...props}
            renderEntity={renderEntity}
            entityTypes={[EntityType.DataPlatform]}
            filterName={t('filtersV2.platformFilter.label')}
            dataTestId="filter-platform"
        />
    );
}
