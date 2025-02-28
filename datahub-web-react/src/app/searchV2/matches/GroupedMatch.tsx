import React, { useState } from 'react';

import styled from 'styled-components';

import { MatchedField } from '../../../types.generated';
import { useSearchQuery } from '../../search/context/SearchContext';
import { useEntityRegistry } from '../../useEntityRegistry';

import { MatchesGroupedByFieldName } from './constants';
import { getDescriptionSlice, isDescriptionField, isHighlightableEntityField } from './utils';

const FieldWrapper = styled.div<{ $isClickable: boolean; $color?: string }>`
    border-radius: 50px;
    padding: 5px 8px;
    &:hover {
        background: #c3b8ee;
    }

    ${(props) => props.$isClickable && `cursor: pointer;`}
    ${(props) => props.$color && `color: ${props.$color};`}
`;

type CustomFieldRenderer = (field: MatchedField) => JSX.Element | null;

const RenderedField = ({
    customFieldRenderer,
    field,
}: {
    customFieldRenderer?: CustomFieldRenderer;
    field: MatchedField;
}) => {
    const entityRegistry = useEntityRegistry();
    const query = useSearchQuery()?.trim()?.toLowerCase();
    const customRenderedField = customFieldRenderer?.(field);
    if (customRenderedField) return <>{customRenderedField}</>;
    if (isHighlightableEntityField(field)) {
        return field.entity ? <>{entityRegistry.getDisplayName(field.entity.type, field.entity)}</> : <></>;
    }
    if (isDescriptionField(field) && query) return <>{getDescriptionSlice(field.value, query)}</>;
    return <>{field.value}</>;
};

interface Props {
    groupedMatch: MatchesGroupedByFieldName;
    limit: number;
    customFieldRenderer?: CustomFieldRenderer;
    onClick?: (query?: string) => void;
    isClickable: boolean;
}

export const GroupedMatch = ({ groupedMatch, limit, customFieldRenderer, onClick, isClickable }: Props) => {
    const [isShowingMore, setIsShowingMore] = useState(false);
    const count = groupedMatch.matchedFields.length;
    const moreCount = Math.max(count - limit, 0);
    const fields = isShowingMore ? groupedMatch.matchedFields : groupedMatch.matchedFields.slice(0, limit);
    return (
        <>
            {fields.map((field) => (
                <FieldWrapper onClick={() => onClick?.(field.value)} $isClickable={isClickable}>
                    <RenderedField field={field} customFieldRenderer={customFieldRenderer} />
                </FieldWrapper>
            ))}
            {!!moreCount && !isShowingMore && (
                <FieldWrapper $isClickable onClick={() => setIsShowingMore(true)} $color="#5C3FD1">
                    + {moreCount} more
                </FieldWrapper>
            )}
            {isShowingMore && (
                <FieldWrapper $isClickable onClick={() => setIsShowingMore(false)} $color="#5C3FD1">
                    Show less
                </FieldWrapper>
            )}
        </>
    );
};
