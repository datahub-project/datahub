import React, { ReactNode } from 'react';

import { Typography } from 'antd';
import styled from 'styled-components';
import { TagSummary } from '../entity/dataset/shared/TagSummary';
import { TermSummary } from '../entity/dataset/shared/TermSummary';
import { useMatchedFields } from './context/SearchResultContext';
import { MatchedField } from '../../types.generated';
import { ANTD_GRAY_V2 } from '../entity/shared/constants';
import { useSearchQuery } from './context/SearchContext';
import { FIELDS_TO_HIGHLIGHT } from './context/constants';
import { getMatchesPrioritizingPrimary } from './context/utils';

// todo - modify this component to match the designs first
// then, we can generalize it to all search cards

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

const MATCH_GROUP_LIMIT = 3;

type Props = {
    fieldRenderer?: (field: MatchedField) => ReactNode;
};

// todo - rename/move this to a generic location
export const MatchedFieldList = ({ fieldRenderer }: Props) => {
    const query = useSearchQuery()?.trim().toLowerCase();
    const matchedFields = useMatchedFields();
    const groupedMatches = getMatchesPrioritizingPrimary(matchedFields, LABEL_INDEX_NAME);

    // todo - implement tooltip but limit rendering to only 10
    const renderField = (field: MatchedField) => {
        const customRenderedField = fieldRenderer?.(field);
        if (customRenderedField) return customRenderedField;
        if (field.value.includes('urn:li:tag')) return <TagSummary urn={field.value} mode="text" />;
        if (field.value.includes('urn:li:glossaryTerm')) return <TermSummary urn={field.value} mode="text" />;
        if (field.name.toLowerCase().includes('description') && query) {
            const queryIndex = field.value.indexOf(query);
            const start = Math.max(0, queryIndex - 10);
            const end = Math.min(field.value.length, queryIndex + query.length + 10);
            return <b>...{field.value.slice(start, end)}...</b>;
        }
        return <b>{field.value}</b>;
    };

    return (
        <>
            {groupedMatches.length > 0 ? (
                <MatchesContainer>
                    {groupedMatches.map((groupedMatch) => {
                        const moreCount = Math.max(groupedMatch.matchedFields.length - MATCH_GROUP_LIMIT, 0);
                        return (
                            <MatchText key={groupedMatch.fieldName}>
                                Matches {FIELDS_TO_HIGHLIGHT.get(groupedMatch.fieldName)}
                                {groupedMatch.matchedFields.length > 1 && 's'}{' '}
                                {groupedMatch.matchedFields.slice(0, MATCH_GROUP_LIMIT).map((field, index) => {
                                    return (
                                        <>
                                            {index > 0 && ', '}
                                            <>{renderField(field)}</>
                                        </>
                                    );
                                })}
                                {moreCount > 0 && (
                                    <>
                                        {' '}
                                        & <b>{moreCount} more</b>
                                    </>
                                )}
                            </MatchText>
                        );
                    })}
                </MatchesContainer>
            ) : null}
        </>
    );
};
