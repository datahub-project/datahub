import React, { useMemo } from 'react';
import styled from 'styled-components';

import { FontColorLevelOptions, FontColorOptions } from '@components/theme/config';

import Match from '@app/searchV2/autoCompleteV2/components/matches/components/Match';
import MoreMatches from '@app/searchV2/autoCompleteV2/components/matches/components/MoreMatches';
import { getMatchesPrioritized, shouldShowInMatchedFieldList } from '@app/searchV2/matches/utils';
import OverflowList from '@src/app/shared/OverflowList';
import { Entity, MatchedField } from '@src/types.generated';

interface Props {
    entity: Entity;
    query?: string;
    displayName?: string;
    matchedFields?: MatchedField[];
    color?: FontColorOptions;
    colorLevel?: FontColorLevelOptions;
}

const MatchedFieldsContainer = styled.div`
    display: flex;
    flex-direction: row;
    overflow-x: hidden;
    text-overflow: ellipsis;
    word-wrap: wrap;
`;

export default function Matches({ entity, query, displayName, matchedFields, color, colorLevel }: Props) {
    const isQueryMatchDisplayName = useMemo(
        () => displayName && query && displayName.toLocaleLowerCase().includes(query.toLocaleLowerCase()),
        [query, displayName],
    );

    const groupedMatchedFields = useMemo(() => {
        const showableFields = (matchedFields ?? []).filter((field) =>
            shouldShowInMatchedFieldList(entity.type, field),
        );

        return getMatchesPrioritized(entity.type, query ?? '', showableFields, 'fieldLabels');
    }, [entity, matchedFields, query]);

    const items = useMemo(() => {
        return groupedMatchedFields.map((match) => ({
            key: match.fieldName,
            node: (
                <Match
                    query={query ?? ''}
                    entityType={entity.type}
                    entity={entity}
                    match={match}
                    color={color}
                    colorLevel={colorLevel}
                />
            ),
        }));
    }, [groupedMatchedFields, query, entity, color, colorLevel]);

    // do not show matched fields if query has matches with display name of entity
    if (isQueryMatchDisplayName) return null;

    if (!items) return null;

    return (
        <MatchedFieldsContainer>
            <OverflowList
                items={items}
                gap={8}
                justifyContent="start"
                alignItems="center"
                renderHiddenItems={(hiddenItems) => (
                    <MoreMatches items={hiddenItems} color={color} colorLevel={colorLevel} />
                )}
            />
        </MatchedFieldsContainer>
    );
}
