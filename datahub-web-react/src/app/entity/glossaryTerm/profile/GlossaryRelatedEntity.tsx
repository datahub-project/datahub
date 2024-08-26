import * as React from 'react';
import { useTranslation } from 'react-i18next';
import { UnionType } from '../../../search/utils/constants';
import { EmbeddedListSearchSection } from '../../shared/components/styled/search/EmbeddedListSearchSection';

import { useEntityData } from '../../shared/EntityContext';

export default function GlossaryRelatedEntity() {
    const { t } = useTranslation();
    const { entityData } = useEntityData();

    const entityUrn = entityData?.urn;

    const fixedOrFilters =
        (entityUrn && [
            {
                field: 'glossaryTerms',
                values: [entityUrn],
            },
            {
                field: 'fieldGlossaryTerms',
                values: [entityUrn],
            },
        ]) ||
        [];

    entityData?.isAChildren?.relationships.forEach((term) => {
        const childUrn = term.entity?.urn;

        if (childUrn) {
            fixedOrFilters.push({
                field: 'glossaryTerms',
                values: [childUrn],
            });

            fixedOrFilters.push({
                field: 'fieldGlossaryTerms',
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
            placeholderText={t('placeholder.filterWithName', { name: t('common.entities') })}
            skipCache
            applyView
        />
    );
}
