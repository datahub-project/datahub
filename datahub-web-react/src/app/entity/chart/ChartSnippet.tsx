import React from 'react';

import { Typography } from 'antd';
import { InputFields, MatchedField, Maybe } from '../../../types.generated';
import TagTermGroup from '../../shared/tags/TagTermGroup';
import { FIELDS_TO_HIGHLIGHT } from '../dataset/search/highlights';
import { getMatchPrioritizingPrimary } from '../shared/utils';

type Props = {
    matchedFields: MatchedField[];
    inputFields: Maybe<InputFields> | undefined;
};

const LABEL_INDEX_NAME = 'fieldLabels';

export const ChartSnippet = ({ matchedFields, inputFields }: Props) => {
    const matchedField = getMatchPrioritizingPrimary(matchedFields, 'fieldLabels');

    if (matchedField?.name === LABEL_INDEX_NAME) {
        const matchedSchemaField = inputFields?.fields?.find(
            (field) => field?.schemaField?.label === matchedField.value,
        );
        const matchedGlossaryTerm = matchedSchemaField?.schemaField?.glossaryTerms?.terms?.find(
            (term) => term?.term?.name === matchedField.value,
        );

        if (matchedGlossaryTerm) {
            return (
                <Typography.Text>
                    Matches metric <TagTermGroup uneditableGlossaryTerms={{ terms: [matchedGlossaryTerm] }} />
                </Typography.Text>
            );
        }
    }

    return matchedField ? (
        <Typography.Text>
            Matches {FIELDS_TO_HIGHLIGHT.get(matchedField.name)} <b>{matchedField.value}</b>
        </Typography.Text>
    ) : null;
};
