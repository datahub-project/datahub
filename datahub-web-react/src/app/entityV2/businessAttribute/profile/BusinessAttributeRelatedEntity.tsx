import * as React from 'react';
import { useTranslation } from 'react-i18next';

import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '@app/search/utils/constants';
import { useEntityData } from '@src/app/entity/shared/EntityContext';

export default function BusinessAttributeRelatedEntity() {
    const { t } = useTranslation('entity.types');
    const { entityData } = useEntityData();

    const entityUrn = entityData?.urn;

    const fixedOrFilters =
        (entityUrn && [
            {
                field: 'businessAttribute',
                values: [entityUrn],
            },
        ]) ||
        [];

    entityData?.isAChildren?.relationships?.forEach((businessAttribute) => {
        const childUrn = businessAttribute.entity?.urn;

        if (childUrn) {
            fixedOrFilters.push({
                field: 'businessAttributes',
                values: [childUrn],
            });
        }
    });

    return (
        <EmbeddedListSearchSection
            fixedFilters={{
                unionType: UnionType.OR,
                filters: fixedOrFilters,
            }}
            emptySearchQuery="*"
            placeholderText={t('shared.filterEntitiesPlaceholder')}
            skipCache
            applyView
        />
    );
}
