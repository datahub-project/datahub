import * as React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SearchCardContext } from '@app/entityV2/shared/SearchCardContext';
import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '@app/search/utils/constants';

export default function GlossaryRelatedEntity() {
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

    entityData?.isAChildren?.relationships?.forEach((term) => {
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
        <SearchCardContext.Provider value={{ showRemovalFromList: true }}>
            <EmbeddedListSearchSection
                fixedFilters={{
                    unionType: UnionType.OR,
                    filters: fixedOrFilters,
                }}
                emptySearchQuery="*"
                placeholderText="Filter assets..."
                skipCache
                applyView
            />
        </SearchCardContext.Provider>
    );
}
