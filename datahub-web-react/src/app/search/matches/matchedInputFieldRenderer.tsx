import React from 'react';

import { Chart, Dashboard, EntityType, GlossaryTerm, MatchedField } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';

const LABEL_INDEX_NAME = 'fieldLabels';
const TYPE_PROPERTY_KEY_NAME = 'type';

const TermName = ({ term }: { term: GlossaryTerm }) => {
    const entityRegistry = useEntityRegistry();
    return <>{entityRegistry.getDisplayName(EntityType.GlossaryTerm, term)}</>;
};

export const matchedInputFieldRenderer = (matchedField: MatchedField, entity: Chart | Dashboard) => {
    if (matchedField?.name === LABEL_INDEX_NAME) {
        const matchedSchemaField = entity.inputFields?.fields?.find(
            (field) => field?.schemaField?.label === matchedField.value,
        );
        const matchedGlossaryTerm = matchedSchemaField?.schemaField?.glossaryTerms?.terms?.find(
            (term) => term?.term?.name === matchedField.value,
        );

        if (matchedGlossaryTerm) {
            let termType = 'term';
            const typeProperty = matchedGlossaryTerm.term.properties?.customProperties?.find(
                (property) => property.key === TYPE_PROPERTY_KEY_NAME,
            );
            if (typeProperty) {
                termType = typeProperty.value || termType;
            }

            return (
                <>
                    {termType} <TermName term={matchedGlossaryTerm.term} />
                </>
            );
        }
    }
    return null;
};
