import React from 'react';

import { Typography } from 'antd';
import { Tooltip } from '@components';
import styled from 'styled-components';
import * as QueryString from 'query-string';
import { useHistory } from 'react-router';

import { useEntityType, useMatchedFieldsForList, useSearchResult } from '../../search/context/SearchResultContext';
import { MatchedField } from '../../../types.generated';
import { useSearchContext } from '../../search/context/SearchContext';
import { useEntityRegistry } from '../../useEntityRegistry';

import { getColumnsTabUrlPath, getMatchedFieldLabel } from './utils';
import { pluralize } from '../../shared/textUtil';
import { SchemaFilterType } from '../../entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import { GroupedMatch } from './GroupedMatch';

const MatchesContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
`;

const MatchContainer = styled.div`
    display: flex;
    padding: 0px 2px;
    align-items: center;
    border-radius: 30px;
    background: #ebe9f4;
    margin-right: 4px;
    white-space: nowrap;
`;

const MatchHeader = styled(Typography.Text)`
    display: flex;
    padding: 4px 2px 4px 10px;
    align-items: center;
    gap: 4px;
    color: #6c6b88;
    font-family: Mulish;
    font-size: 12px;
    font-style: normal;
    line-height: normal;
`;

const MatchText = styled(Typography.Text)`
    display: flex;
    justify-content: center;
    align-items: center;
    border-radius: 50px;
    background: #ebe9f4;
    color: #374066;
    font-family: Mulish;
    font-size: 12px;
    font-style: normal;
    font-weight: 400;
    line-height: normal;
`;

const MATCH_GROUP_LIMIT = 3;

const CLICKABLE_FIELDS = {
    fieldPaths: SchemaFilterType.FieldPath,
    fieldDescriptions: SchemaFilterType.Documentation,
    fieldTags: SchemaFilterType.Tags,
    fieldGlossaryTerms: SchemaFilterType.Terms,
};

type CustomFieldRenderer = (field: MatchedField) => JSX.Element | null;

type Props = {
    customFieldRenderer?: CustomFieldRenderer;
    matchSuffix?: string;
};

export const MatchedFieldList = ({ customFieldRenderer, matchSuffix }: Props) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const searchContext = useSearchContext();
    const result = useSearchResult();
    const groupedMatches = useMatchedFieldsForList('fieldLabels');
    const entityType = useEntityType();

    if (!groupedMatches) return null;

    return (
        <MatchesContainer>
            {groupedMatches.map((groupedMatch) => {
                const label = getMatchedFieldLabel(entityType, groupedMatch.fieldName);
                const isClickable = Object.keys(CLICKABLE_FIELDS).includes(groupedMatch.fieldName);
                const onClick = (query?: string) => {
                    if (result?.entity?.type && result?.entity?.urn && isClickable) {
                        let matchedText = query;
                        if (!query || query.startsWith('urn:li:')) {
                            matchedText = searchContext.query || '';
                        }
                        const columnsTabPath = getColumnsTabUrlPath(result.entity.type);
                        history.push(
                            `${entityRegistry.getEntityUrl(
                                result.entity.type,
                                result.entity.urn,
                            )}/${columnsTabPath}?${QueryString.stringify({
                                matchedText,
                            })}`,
                        );
                    }
                };
                return (
                    <Tooltip
                        title={
                            matchSuffix
                                ? `Matches ${pluralize(groupedMatch.matchedFields.length, label)} ${matchSuffix}`
                                : undefined
                        }
                    >
                        <MatchContainer>
                            <MatchHeader>
                                <b>Matches:</b>
                                {pluralize(groupedMatch.matchedFields.length, label)}
                            </MatchHeader>
                            <MatchText key={groupedMatch.fieldName}>
                                <GroupedMatch
                                    groupedMatch={groupedMatch}
                                    limit={MATCH_GROUP_LIMIT}
                                    customFieldRenderer={customFieldRenderer}
                                    onClick={onClick}
                                    isClickable={isClickable}
                                />
                            </MatchText>
                        </MatchContainer>
                    </Tooltip>
                );
            })}
        </MatchesContainer>
    );
};
