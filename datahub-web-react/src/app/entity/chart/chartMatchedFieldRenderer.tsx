import React from 'react';

import { Chart, Dashboard, MatchedField } from '../../../types.generated';
import TagTermGroup from '../../shared/tags/TagTermGroup';

const LABEL_INDEX_NAME = 'fieldLabels';
const TYPE_PROPERTY_KEY_NAME = 'type';

// todo - move this file somewhere, maybe utils under chart?
export const chartMatchedFieldRenderer = (matchedField: MatchedField, entity: Chart | Dashboard) => {
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

            // todo - need to clean up this list view down here, just want text tbh
            return (
                <>
                    {termType} <TagTermGroup uneditableGlossaryTerms={{ terms: [matchedGlossaryTerm] }} />{' '}
                </>
            );
        }
    }
    return null;
};
