import React from 'react';
import { useEntityData } from '../shared/EntityContext';
import { EmbeddedListSearchSection } from '../shared/components/styled/search/EmbeddedListSearchSection';
import generateUseListDataProductAssets from './generateUseListDataProductAssets';
import { useTranslation } from 'react-i18next';

export function DataProductEntitiesTab() {
    const { t } = useTranslation();
    const { urn } = useEntityData();

    return (
        <EmbeddedListSearchSection
            useGetSearchResults={generateUseListDataProductAssets({ urn })}
            emptySearchQuery="*"
            placeholderText={t('placeholder.filterWithName', { name: t('common.assets') })}
        />
    );
}
