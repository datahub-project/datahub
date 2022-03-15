import * as React from 'react';
import RelatedEntityResults from '../../../shared/entitySearch/RelatedEntityResults';

import { useEntityData } from '../../shared/EntityContext';

export default function GlossaryRelatedEntity() {
    const { entityData }: any = useEntityData();
    const glossaryTermHierarchicalName = entityData?.hierarchicalName;
    const fixedQueryString = `glossaryTerms:"${glossaryTermHierarchicalName}" OR fieldGlossaryTerms:"${glossaryTermHierarchicalName}" OR editedFieldGlossaryTerms:"${glossaryTermHierarchicalName}"`;

    return <RelatedEntityResults fixedQuery={fixedQueryString} />;
}
