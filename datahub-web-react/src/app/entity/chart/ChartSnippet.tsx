import React from 'react';

import { Typography } from 'antd';
import styled from 'styled-components';
import { InputFields, MatchedField, Maybe } from '../../../types.generated';
import TagTermGroup from '../../shared/tags/TagTermGroup';
import { FIELDS_TO_HIGHLIGHT } from '../dataset/search/highlights';
import { getMatchesPrioritizingPrimary } from '../shared/utils';
import { ANTD_GRAY_V2 } from '../shared/constants';

type Props = {
    matchedFields: MatchedField[];
    inputFields: Maybe<InputFields> | undefined;
};

const LABEL_INDEX_NAME = 'fieldLabels';
const TYPE_PROPERTY_KEY_NAME = 'type';

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

// todo - do we render out chart labels on the search card today? if so, maybe we highlight those?
export const ChartSnippet = ({ matchedFields, inputFields }: Props) => {
    const groupedMatches = getMatchesPrioritizingPrimary(matchedFields, LABEL_INDEX_NAME);

    const renderField = (matchedField: MatchedField) => {
        if (matchedField?.name === LABEL_INDEX_NAME) {
            const matchedSchemaField = inputFields?.fields?.find(
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
                        {termType} <TagTermGroup uneditableGlossaryTerms={{ terms: [matchedGlossaryTerm] }} />{' '}
                    </>
                );
            }
        }
        return <b>{matchedField.value}</b>;
    };

    return (
        <>
            {groupedMatches.length > 0 ? (
                <MatchesContainer>
                    {groupedMatches.map((groupedMatch) => (
                        <MatchText>
                            Matches {FIELDS_TO_HIGHLIGHT.get(groupedMatch.fieldName)}{' '}
                            {groupedMatch.matchedFields.map((field, index) => (
                                <>
                                    {index > 0 && ', '}
                                    <span>{renderField(field)}</span>
                                </>
                            ))}{' '}
                        </MatchText>
                    ))}
                </MatchesContainer>
            ) : null}
        </>
    );
};
