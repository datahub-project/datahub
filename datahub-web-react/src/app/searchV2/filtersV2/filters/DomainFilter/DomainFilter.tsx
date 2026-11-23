import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';

import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { DomainSelectOption } from '@app/sharedV2/domains/DomainSelectOption';
import { Entity, EntityType } from '@src/types.generated';

export default function DomainFilter(props: FilterComponentProps) {
    const { t } = useTranslation('search');
    const renderEntity = useCallback((entity: Entity) => <DomainSelectOption entity={entity} />, []);

    return (
        <BaseEntityFilter
            {...props}
            renderEntity={renderEntity}
            entityTypes={[EntityType.Domain]}
            filterName={t('filtersV2.domainFilter.label')}
            dataTestId="filter-domain"
        />
    );
}
