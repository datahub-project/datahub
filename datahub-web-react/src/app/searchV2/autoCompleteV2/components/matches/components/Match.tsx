import React, { useMemo } from 'react';
import styled from 'styled-components';

import { removeMarkdown } from '@app/entity/shared/components/styled/StripMarkdownText';
import { isChart } from '@app/entityV2/chart/utils';
import { isDashboard } from '@app/entityV2/dashboard/utils';
import { downgradeV2FieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { matchedInputFieldParams } from '@app/search/matches/matchedInputFieldRenderer';
import { MATCH_COLOR, MATCH_COLOR_LEVEL } from '@app/searchV2/autoCompleteV2/constants';
import { getDescriptionSlice, isDescriptionField, isHighlightableEntityField } from '@app/searchV2/matches/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { MatchText, Text } from '@src/alchemy-components';
import { MatchesGroupedByFieldName } from '@src/app/search/matches/constants';
import { getMatchedFieldLabel } from '@src/app/search/matches/utils';
import { capitalizeFirstLetterOnly } from '@src/app/shared/textUtil';
import { Entity, EntityType } from '@src/types.generated';

const TextWrapper = styled.span`
    overflow: hidden;
    word-wrap: wrap;
    text-overflow: ellipsis;
`;

interface Props {
    query: string;
    entityType: EntityType;
    entity: Entity;
    match: MatchesGroupedByFieldName;
}

export default function Match({ query, entityType, entity, match }: Props) {
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
            return field.entity ? entityRegistry.getDisplayName(field.entity.type, field.entity) : '';
        }

        if (entityType === EntityType.Dataset && field.name === 'fieldPaths') {
            return downgradeV2FieldPath(field.value);
        }

        if (isChart(entity) || isDashboard(entity)) {
            const { termType, term } = matchedInputFieldParams(field, entity);

            if (termType && term) {
                return `${termType} ${entityRegistry.getDisplayName(term.type, term)}`;
            }
        }

        return field.value;
    }, [match, query, entityRegistry, entity, entityType]);

    if (!value) return null;

    return (
        <TextWrapper>
            <Text color={MATCH_COLOR} colorLevel={MATCH_COLOR_LEVEL} size="sm" type="span">
                {label}: <MatchText size="sm" type="span" text={value} highlight={query} />
            </Text>
        </TextWrapper>
    );
}
