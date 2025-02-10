import React from 'react';

import { Tooltip, Typography } from 'antd';
import styled from 'styled-components';
import { useMatchedFieldLabel, useMatchedFieldsForList } from '../context/SearchResultContext';
import { MatchedField } from '../../../types.generated';
import { ANTD_GRAY_V2 } from '../../entity/shared/constants';
import { useSearchQuery } from '../context/SearchContext';
import { MatchesGroupedByFieldName } from './constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getDescriptionSlice, isDescriptionField, isHighlightableEntityField } from './utils';

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
const TOOLTIP_MATCH_GROUP_LIMIT = 10;

type CustomFieldRenderer = (field: MatchedField) => JSX.Element | null;

type Props = {
    customFieldRenderer?: CustomFieldRenderer;
    matchSuffix?: string;
};

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
    if (customRenderedField) return <b>{customRenderedField}</b>;
    if (isHighlightableEntityField(field)) {
        return field.entity ? <>{entityRegistry.getDisplayName(field.entity.type, field.entity)}</> : <></>;
    }
    if (isDescriptionField(field) && query) return <b>{getDescriptionSlice(field.value, query)}</b>;
    return <b>{field.value}</b>;
};

const MatchedFieldsList = ({
    groupedMatch,
    limit,
    tooltip,
    matchSuffix = '',
    customFieldRenderer,
}: {
    groupedMatch: MatchesGroupedByFieldName;
    limit: number;
    tooltip?: JSX.Element;
    matchSuffix?: string;
    customFieldRenderer?: CustomFieldRenderer;
}) => {
    const label = useMatchedFieldLabel(groupedMatch.fieldName);
    const count = groupedMatch.matchedFields.length;
    const moreCount = Math.max(count - limit, 0);
    const andMore = (
        <>
            {' '}
            & <b>more</b>
        </>
    );
    return (
        <>
            Matches {count > 1 && `${count} `}
            {label}
            {count > 1 && 's'}{' '}
            {groupedMatch.matchedFields.slice(0, limit).map((field, index) => (
                <>
                    {index > 0 && ', '}
                    <>
                        <RenderedField field={field} customFieldRenderer={customFieldRenderer} />
                    </>
                </>
            ))}
            {moreCount > 0 &&
                (tooltip ? (
                    <Tooltip title={tooltip} placement="bottom" mouseEnterDelay={1}>
                        {andMore}
                    </Tooltip>
                ) : (
                    <>{andMore}</>
                ))}{' '}
            {matchSuffix}
        </>
    );
};

export const MatchedFieldList = ({ customFieldRenderer, matchSuffix = '' }: Props) => {
    const groupedMatches = useMatchedFieldsForList('fieldLabels');

    return (
        <>
            {groupedMatches.length > 0 ? (
                <MatchesContainer>
                    {groupedMatches.map((groupedMatch) => {
                        return (
                            <MatchText key={groupedMatch.fieldName}>
                                <MatchedFieldsList
                                    groupedMatch={groupedMatch}
                                    limit={MATCH_GROUP_LIMIT}
                                    customFieldRenderer={customFieldRenderer}
                                    matchSuffix={matchSuffix}
                                    tooltip={
                                        <MatchedFieldsList
                                            groupedMatch={groupedMatch}
                                            limit={TOOLTIP_MATCH_GROUP_LIMIT}
                                            customFieldRenderer={customFieldRenderer}
                                        />
                                    }
                                />
                            </MatchText>
                        );
                    })}
                </MatchesContainer>
            ) : null}
        </>
    );
};
