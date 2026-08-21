import React from 'react';
import { useTranslation } from 'react-i18next';

import EntityAutocompleteDropdown from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/EntityAutocompleteDropdown';

import { EntityType } from '@types';

export default function MarketplaceSearch() {
    const { t } = useTranslation('misc');

    return (
        <EntityAutocompleteDropdown
            entityTypes={[EntityType.DataProduct]}
            placeholder={t('marketplace.searchPlaceholder')}
            dataTestId="marketplace-sidebar-search-input"
        />
    );
}
