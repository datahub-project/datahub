import * as React from 'react';
import { EmbeddedListSearchSection } from '../../shared/components/styled/search/EmbeddedListSearchSection';

import { useEntityData } from '../../shared/EntityContext';

export default function GlossaryRelatedEntity() {
    const { entityData }: any = useEntityData();
    const glossaryTermHierarchicalName = entityData?.hierarchicalName;
    let fixedQueryString = `glossaryTerms:"${glossaryTermHierarchicalName}" OR fieldGlossaryTerms:"${glossaryTermHierarchicalName}" OR editedFieldGlossaryTerms:"${glossaryTermHierarchicalName}"`;
    entityData?.isAChildren?.relationships.forEach((term) => {
        const name = term.entity?.hierarchicalName;
        fixedQueryString += `OR glossaryTerms:"${name}" OR fieldGlossaryTerms:"${name}" OR editedFieldGlossaryTerms:"${name}"`;
    });

    return (
        <EmbeddedListSearchSection
            fixedQuery={fixedQueryString}
            emptySearchQuery="*"
            placeholderText="Filter entities..."
        />
    );
}
