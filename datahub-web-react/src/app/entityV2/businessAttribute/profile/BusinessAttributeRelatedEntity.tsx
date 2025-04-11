import * as React from 'react';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { UnionType } from '../../../search/utils/constants';
import { EmbeddedListSearchSection } from '../../shared/components/styled/search/EmbeddedListSearchSection';

export default function BusinessAttributeRelatedEntity() {
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
            placeholderText="Filter entities..."
            skipCache
            applyView
        />
    );
}
