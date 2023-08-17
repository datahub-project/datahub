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
import { useSearchQuery } from '../../search/context/SearchContext';

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

// todo - revert to 3
const MATCH_GROUP_LIMIT = 1;

export const DatasetSearchSnippet = () => {
    const query = useSearchQuery()?.trim().toLowerCase();
    const matchedFields = useMatchedFields();
    const groupedMatches = getMatchesPrioritizingPrimary(matchedFields, LABEL_INDEX_NAME);

    // todo - implement tooltip but limit rendering to only 10
    const renderField = (field: MatchedField) => {
        // todo - render just the text of the tag/term
        if (field.value.includes('urn:li:tag')) return <TagSummary urn={field.value} mode="text" />;
        if (field.value.includes('urn:li:glossaryTerm')) return <TermSummary urn={field.value} mode="text" />;
        if (field.name === 'fieldPaths') return <b>{downgradeV2FieldPath(field.value)}</b>;
        // todo - for any description column, don't output the whole value, just output the ellipsis match...
        // todo - for dataset description, we already show that, don't need to repeat it down here
        if (field.name.toLowerCase().includes('description') && query) {
            const queryIndex = field.value.indexOf(query);
            const start = Math.max(0, queryIndex - 10);
            const end = Math.min(field.value.length, queryIndex + query.length + 10);
            const value = field.value.slice(start, end);
            return <b>...{value}...</b>;
        }
        return <b>{field.value}</b>;
    };

    return (
        <>
            {groupedMatches.length > 0 ? (
                <MatchesContainer>
                    {groupedMatches.map((groupedMatch) => (
                        <MatchText key={groupedMatch.fieldName}>
                            Matches {FIELDS_TO_HIGHLIGHT.get(groupedMatch.fieldName)}{' '}
                            {groupedMatch.matchedFields.slice(0, MATCH_GROUP_LIMIT).map((field, index) => {
                                const moreCount = Math.max(groupedMatch.matchedFields.length - MATCH_GROUP_LIMIT, 0);
                                return (
                                    <>
                                        {index > 0 && ', '}
                                        <>{renderField(field)}</>
                                        {moreCount > 0 && (
                                            <>
                                                {' '}
                                                & <b>{moreCount} more</b>
                                            </>
                                        )}
                                    </>
                                );
                            })}
                        </MatchText>
                    ))}
                </MatchesContainer>
            ) : null}
        </>
    );
};
