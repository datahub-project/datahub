import React from 'react';
import { useTranslation } from 'react-i18next';

import EntityAutocompleteDropdown from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/EntityAutocompleteDropdown';

import { EntityType } from '@types';

export default function GlossarySearch() {
    const { t: tc } = useTranslation('common.actions');

    return (
        <EntityAutocompleteDropdown
            entityTypes={[EntityType.GlossaryTerm, EntityType.GlossaryNode]}
            placeholder={tc('search')}
        />
    );
}
