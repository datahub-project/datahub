import React from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '@app/search/utils/constants';

export const ContainerEntitiesTab = () => {
    const { t } = useTranslation('entity.types');
    const { urn } = useEntityData();

    const fixedFilter = {
        field: 'container',
        values: [urn],
    };

    return (
        <EmbeddedListSearchSection
            fixedFilters={{
                unionType: UnionType.AND,
                filters: [fixedFilter],
            }}
            emptySearchQuery="*"
            placeholderText={t('container.filterEntitiesPlaceholder')}
            applyView
        />
    );
};
