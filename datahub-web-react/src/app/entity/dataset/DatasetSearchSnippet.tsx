import React from 'react';

import { Typography } from 'antd';
import { MatchedField } from '../../../types.generated';
import { TagSummary } from './shared/TagSummary';
import { TermSummary } from './shared/TermSummary';
import { FIELDS_TO_HIGHLIGHT } from './search/highlights';
import { getMatchPrioritizingPrimary } from '../shared/utils';

type Props = {
    matchedFields: MatchedField[];
};

const LABEL_INDEX_NAME = 'fieldLabels';

export const DatasetSearchSnippet = ({ matchedFields }: Props) => {
    const matchedField = getMatchPrioritizingPrimary(matchedFields, LABEL_INDEX_NAME);

    let snippet: React.ReactNode;

    if (matchedField) {
        if (matchedField.value.includes('urn:li:tag')) {
            snippet = <TagSummary urn={matchedField.value} />;
        } else if (matchedField.value.includes('urn:li:glossaryTerm')) {
            snippet = <TermSummary urn={matchedField.value} />;
        } else {
            snippet = <b>{matchedField.value}</b>;
        }
    }

    return matchedField ? (
        <Typography.Text>
            Matches {FIELDS_TO_HIGHLIGHT.get(matchedField.name)} {snippet}{' '}
        </Typography.Text>
    ) : null;
};
