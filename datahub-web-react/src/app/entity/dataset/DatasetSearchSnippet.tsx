import React from 'react';

import { Typography } from 'antd';
import styled from 'styled-components';
import { TagSummary } from './shared/TagSummary';
import { TermSummary } from './shared/TermSummary';
import { FIELDS_TO_HIGHLIGHT } from './search/highlights';
import { getMatchesPrioritizingPrimary } from '../shared/utils';
import { downgradeV2FieldPath } from './profile/schema/utils/utils';
import { useMatchedFields } from '../../search/context/SearchResultContext';
import { MatchedField } from '../../../types.generated';
import { ANTD_GRAY_V2 } from '../shared/constants';

// todo - modify this component to match the designs first
// then, we can generalize it to all search cards
// then, we can remove the ChartSnippet

const LABEL_INDEX_NAME = 'fieldLabels';

const MatchesContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
`;

const MatchText = styled(Typography.Text)`
    color: ${ANTD_GRAY_V2[8]};
    background: ${(props) => props.theme.styles['highlight-color']};
    border-radius: 4px;
    padding: 2px 4px 2px 4px;
    padding-right: 4px;
`;

export const DatasetSearchSnippet = () => {
    const matchedFields = useMatchedFields();
    const matchedFieldsPrioritized = getMatchesPrioritizingPrimary(matchedFields, LABEL_INDEX_NAME);

    // todo - group by field somehow?
    // ie. columns into a csv
    // need, a map of match.field=>[...matchedFields]
    const renderSnippet = (field: MatchedField) => {
        if (field.value.includes('urn:li:tag')) return <TagSummary urn={field.value} />;
        if (field.value.includes('urn:li:glossaryTerm')) return <TermSummary urn={field.value} />;
        if (field.name === 'fieldPaths') return <b>{downgradeV2FieldPath(field.value)}</b>;
        return <b>{field.value}</b>;
    };

    return (
        <>
            {matchedFieldsPrioritized.length > 0 ? (
                <MatchesContainer>
                    {matchedFieldsPrioritized.map((field) => (
                        <MatchText>
                            Matches {FIELDS_TO_HIGHLIGHT.get(field.name)} {renderSnippet(field)}{' '}
                        </MatchText>
                    ))}
                </MatchesContainer>
            ) : null}
        </>
    );
};
