import { mergeWith } from 'lodash';

import { GlossaryTerm, Maybe, PromptCardinality } from '@src/types.generated';

// Get default value of glossary terms based on different conditions to show in the form
export const getDefaultTermEntities = (
    existingTerms: GlossaryTerm[],
    cardinality: PromptCardinality,
    allowedTerms: Maybe<GlossaryTerm[]> | undefined,
) => {
    const allowedTermsUrns = allowedTerms?.map((term) => term.urn);
    const areTermsRestricted = (allowedTerms && allowedTerms?.length > 0) ?? false;

    if (existingTerms.length > 0) {
        if (cardinality === PromptCardinality.Multiple && !areTermsRestricted) {
            return existingTerms;
        }
        if (cardinality === PromptCardinality.Multiple && areTermsRestricted) {
            return existingTerms.filter((term) => allowedTermsUrns?.includes(term.urn));
        }
        if (cardinality === PromptCardinality.Single && !areTermsRestricted) {
            return existingTerms[0] ? [existingTerms[0]] : [];
        }
        if (cardinality === PromptCardinality.Single && areTermsRestricted) {
            const defaultTerms = existingTerms.filter((term) => allowedTermsUrns?.includes(term.urn));
            return defaultTerms.length > 0 ? [defaultTerms[0]] : [];
        }
    }
    return [];
};

export const mergeObjects = (object1, object2) => {
    const mergedObject = mergeWith({}, object1, object2, (firstValue, secondValue) => {
        if (Array.isArray(firstValue) && Array.isArray(secondValue)) {
            return firstValue.concat(secondValue);
        }
        return undefined;
    });
    return mergedObject;
};
