import { NodeViewComponentProps } from '@remirror/react';
import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Pill } from '@components/components/Pills';
import { Tooltip } from '@components/components/Tooltip';

import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetEntityMentionNodeQuery } from '@src/graphql/search.generated';
import type { Entity } from '@src/types.generated';

/** Matches Avatar `sm` / mention chip icon slot. */
const INLINE_ICON_SIZE = 18;

const ChipWrap = styled.span`
    display: inline-flex;
    max-width: 100%;
    vertical-align: middle;
    margin: 0 1px;
`;

const IconSlot = styled.span`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: ${INLINE_ICON_SIZE}px;
    height: ${INLINE_ICON_SIZE}px;
    flex-shrink: 0;
    line-height: 0;

    & .ant-image {
        display: flex !important;
        align-items: center;
        justify-content: center;
        width: ${INLINE_ICON_SIZE}px !important;
        height: ${INLINE_ICON_SIZE}px !important;
        margin: 0 !important;
    }

    svg,
    img {
        display: block;
        max-width: 100%;
        max-height: 100%;
    }
`;

const InvalidEntityText = styled.span`
    font-weight: 500;
    color: ${(props) => props.theme.colors.textDisabled};
`;

/**
 * Renders a datahubMention chip in the editor via alchemy {@link Pill} (`squareFilled`)
 * and the same {@link EntityIcon} treatment as the @-mention picker.
 */
export const MentionsNodeView = ({ node }: NodeViewComponentProps) => {
    const { t } = useTranslation('entity.profile.editor');
    const { urn, name } = node.attrs;

    const registry = useEntityRegistry();
    const { data, loading } = useGetEntityMentionNodeQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const renderIcon = useCallback(
        (entity: Entity): React.ReactNode => (
            <IconSlot aria-hidden>
                <EntityIcon entity={entity} size={INLINE_ICON_SIZE} />
            </IconSlot>
        ),
        [],
    );

    if (loading) {
        return <span>{name}</span>;
    }

    if (!data || !data.entity) {
        return (
            <Tooltip title={t('mentions.notFound')}>
                <InvalidEntityText>{name}</InvalidEntityText>
            </Tooltip>
        );
    }

    const entity = data.entity as Entity;
    const displayName = registry.getDisplayName(entity.type, entity);

    return (
        <HoverEntityTooltip entity={entity}>
            <ChipWrap>
                <Pill
                    label={displayName}
                    color="gray"
                    size="sm"
                    variant="squareFilled"
                    clickable={false}
                    customIconRenderer={() => renderIcon(entity)}
                />
            </ChipWrap>
        </HoverEntityTooltip>
    );
};
