import React, { useMemo } from 'react';
import styled from 'styled-components';

import { removeMarkdown } from '@app/entity/shared/components/styled/StripMarkdownText';
import { MATCH_COLOR, MATCH_COLOR_LEVEL } from '@app/searchV2/autoCompleteV2/constants';
import { getDescriptionSlice, isDescriptionField, isHighlightableEntityField } from '@app/searchV2/matches/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { MatchText, Text } from '@src/alchemy-components';
import { MatchesGroupedByFieldName } from '@src/app/search/matches/constants';
import { getMatchedFieldLabel } from '@src/app/search/matches/utils';
import { capitalizeFirstLetterOnly } from '@src/app/shared/textUtil';
import { EntityType } from '@src/types.generated';

const TextWrapper = styled.span`
    overflow: hidden;
    word-wrap: wrap;
    text-overflow: ellipsis;
`;

interface Props {
    query: string;
    entityType: EntityType;
    match: MatchesGroupedByFieldName;
}

export default function Match({ query, entityType, match }: Props) {
    const entityRegistry = useEntityRegistryV2();

    const label = useMemo(
        () => capitalizeFirstLetterOnly(getMatchedFieldLabel(entityType, match.fieldName)),
        [entityType, match.fieldName],
    );

    const value = useMemo(() => {
        // show only the first value
        const field = match.matchedFields?.[0];
        if (field === undefined) return undefined;

        // do not show empty matches
        if (field.value === '') return undefined;

        if (isDescriptionField(field) && query) {
            const cleanedValue: string = removeMarkdown(field.value);

            // do not show the description if it doesn't include query
            if (!cleanedValue.toLowerCase().includes(query.toLocaleLowerCase())) return undefined;

            return getDescriptionSlice(cleanedValue, query);
        }

        if (isHighlightableEntityField(field)) {
            console.log('>>> match', field);
            return field.entity ? entityRegistry.getDisplayName(field.entity.type, field.entity) : '';
        }

        return field.value;
    }, [match, query, entityRegistry]);

    if (value === undefined) return null;

    return (
        <TextWrapper>
            <Text color={MATCH_COLOR} colorLevel={MATCH_COLOR_LEVEL} size="sm" type="span">
                {label}: <MatchText size="sm" type="span" text={value} highlight={query} />
            </Text>
        </TextWrapper>
    );
}
