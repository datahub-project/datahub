import React from 'react';

import { Typography } from 'antd';
import * as QueryString from 'query-string';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { MatchedField } from '../../../types.generated';
import { SEARCH_COLORS } from '../../entityV2/shared/constants';
import { SchemaFilterType } from '../../entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import { useSearchContext, useSearchQuery } from '../../search/context/SearchContext';
import {
    useMatchedFieldLabel,
    useMatchedFieldsForList,
    useSearchResult,
} from '../../search/context/SearchResultContext';
import { useEntityRegistry } from '../../useEntityRegistry';
import { MatchesGroupedByFieldName } from './constants';
import { getDescriptionSlice, isDescriptionField, isHighlightableEntityField } from './utils';

const MatchesContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
`;

const MatchTextPadding = styled.span`
    min-width: 24px;
`;

export const MatchText = styled(Typography.Text)<{ isClickable?: boolean; forceHover?: boolean }>`
    padding: 8px;
    padding-right: 8px;
    padding-left: 8px;
    max-width: 140px;
    white-space: nowrap;
    overflow: hidden;

    div {
        text-overflow: ellipsis;
    }

    text-overflow: ellipsis;
    color: ${SEARCH_COLORS.MATCH_TEXT_GREY};
    transition: max-width 0.3s ease;

    :hover {
        background-color: ${SEARCH_COLORS.MATCH_BACKGROUND_GREY};
        color: ${SEARCH_COLORS.TITLE_PURPLE};
        border-radius: 20px;
        max-width: 400px;
        ${(props) => props.isClickable && `cursor: pointer;`};
    }

    ${(props) =>
        props.forceHover &&
        `
        background-color: ${SEARCH_COLORS.MATCH_BACKGROUND_GREY};
        color: ${SEARCH_COLORS.TITLE_PURPLE};
        border-radius: 20px;
        max-width: 400px;
        ${props.isClickable && `cursor: pointer;`};
    `}
`;

const MATCH_GROUP_LIMIT = 3;

type CustomFieldRenderer = (field: MatchedField) => JSX.Element | null;

type Props = {
    customFieldRenderer?: CustomFieldRenderer;
    matchSuffix?: string;
};

const clickableFieldNames = {
    fieldPaths: SchemaFilterType.FieldPath,
    fieldDescriptions: SchemaFilterType.Documentation,
    fieldTags: SchemaFilterType.Tags,
    fieldGlossaryTerms: SchemaFilterType.Terms,
};

const RenderedField = ({
    customFieldRenderer,
    field,
}: {
    customFieldRenderer?: CustomFieldRenderer;
    field: MatchedField;
}) => {
    const entityRegistry = useEntityRegistry();
    const query = useSearchQuery()?.trim().toLowerCase();
    const customRenderedField = customFieldRenderer?.(field);
    if (customRenderedField) return <b>{customRenderedField}</b>;
    if (isHighlightableEntityField(field)) {
        return field.entity ? <>{entityRegistry.getDisplayName(field.entity.type, field.entity)}</> : <></>;
    }
    if (isDescriptionField(field) && query) return <b>{getDescriptionSlice(field.value, query)}</b>;
    return <b>{field.value}</b>;
};

const MatchGroup = styled.span``;

const MatchedFieldsList = ({
    groupedMatch,
    limit,
    matchSuffix = '',
    customFieldRenderer,
}: {
    groupedMatch: MatchesGroupedByFieldName;
    limit: number;
    matchSuffix?: string;
    customFieldRenderer?: CustomFieldRenderer;
}) => {
    const label = useMatchedFieldLabel(groupedMatch.fieldName);
    const count = groupedMatch.matchedFields.length;

    return (
        <MatchGroup>
            {count > 0 && `${count} `}
            {label}
            {count > 1 && 's'}{' '}
            {count < 2 &&
                groupedMatch.matchedFields.slice(0, limit).map((field, index) => (
                    <>
                        {index > 0 && ', '}
                        <span className="renderedField">
                            <RenderedField field={field} customFieldRenderer={customFieldRenderer} />
                        </span>
                    </>
                ))}
            {matchSuffix}
        </MatchGroup>
    );
};

export const MatchedFieldList = ({ customFieldRenderer, matchSuffix = '' }: Props) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const searchContext = useSearchContext();
    const result = useSearchResult();
    const groupedMatches = useMatchedFieldsForList('fieldPaths');

    if (groupedMatches.length === 0) {
        return null;
    }

    return (
        <MatchesContainer>
            {groupedMatches.map((groupedMatch) => {
                const isClickable = Object.keys(clickableFieldNames).includes(groupedMatch.fieldName);
                const onClick = () => {
                    if (result?.entity?.type && result?.entity?.urn && isClickable) {
                        console.log({
                            schemaFilter: searchContext.query || '',
                            schemaFilterTypes: QueryString.stringify([clickableFieldNames[groupedMatch.fieldName]]),
                            stringified: QueryString.stringify({
                                schemaFilter: searchContext.query || '',
                                schemaFilterTypes: [clickableFieldNames[groupedMatch.fieldName]],
                            }),
                            full: `${entityRegistry.getEntityUrl(
                                result.entity.type,
                                result.entity.urn,
                            )}/Columns?${QueryString.stringify({
                                schemaFilter: searchContext.query || '',
                                schemaFilterTypes: [clickableFieldNames[groupedMatch.fieldName]],
                            })}`,
                        });
                        history.push(
                            `${entityRegistry.getEntityUrl(
                                result.entity.type,
                                result.entity.urn,
                            )}/Columns?${QueryString.stringify({
                                schemaFilter: searchContext.query || '',
                                schemaFilterTypes: [clickableFieldNames[groupedMatch.fieldName]],
                            })}`,
                        );
                    }
                };

                return (
                    <MatchText onClick={onClick} isClickable={isClickable} key={groupedMatch.fieldName}>
                        <span>
                            <MatchedFieldsList
                                groupedMatch={groupedMatch}
                                limit={MATCH_GROUP_LIMIT}
                                customFieldRenderer={customFieldRenderer}
                                matchSuffix={matchSuffix}
                            />
                        </span>
                        <MatchTextPadding />
                    </MatchText>
                );
            })}
        </MatchesContainer>
    );
};
