import React from 'react';
import { useTranslation } from 'react-i18next';
import { useEntityData } from '../shared/EntityContext';
import { EmbeddedListSearchSection } from '../shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '../../search/utils/constants';

export const ContainerEntitiesTab = () => {
    const { t } = useTranslation();
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
            placeholderText={t('placeholder.filterWithName', { name: t('common.containers') })}
            applyView
        />
    );
};
