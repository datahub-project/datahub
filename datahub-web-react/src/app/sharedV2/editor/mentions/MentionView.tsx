import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import type { EditorMentionViewProps } from '@components/components/Editor/types';
import { Pill } from '@components/components/Pills';
import { Tooltip } from '@components/components/Tooltip';

import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { useBatchedMentionEntity } from '@app/sharedV2/editor/mentions/MentionEntitiesContext';
import EntityHoverCard from '@app/sharedV2/hoverCard/EntityHoverCard';
import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetEntityMentionChipQuery, useGetEntityMentionNodeQuery } from '@src/graphql/search.generated';
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
 * The chip query carries only the name and icon fields, so the hover card fetches the full entity
 * when it opens. Until that arrives it shows what the chip already has.
 */
function MentionHoverCard({ entity }: { entity: Entity }) {
    const { data } = useGetEntityMentionNodeQuery({
        // The card needs neither lineage nor siblings; the query fetches both unless skipped.
        variables: { urn: entity.urn, skipLineage: true, skipSiblingsSearch: true },
        fetchPolicy: 'cache-first',
    });
    return <EntityHoverCard entity={(data?.entity as Entity | undefined) ?? entity} />;
}

/**
 * Renders a mention chip, in the editor and in read-only documentation, via alchemy {@link Pill}
 * (`squareFilled`) and the same {@link EntityIcon} treatment as the @-mention picker.
 */
export const MentionView = ({ urn, name }: EditorMentionViewProps) => {
    const { t } = useTranslation('entity.profile.editor');

    const registry = useEntityRegistry();
    // Read-only content fetches all its mentions at once; the editor fetches each mention itself.
    const batched = useBatchedMentionEntity(urn);
    const { data, loading: queryLoading } = useGetEntityMentionChipQuery({
        // A chip needs no siblings; the query fetches them unless skipped.
        variables: { urn, skipSiblingsSearch: true },
        fetchPolicy: 'cache-first',
        skip: !!batched,
    });
    const loading = batched ? batched.loading : queryLoading;
    const fetchedEntity = batched ? batched.entity : data?.entity;

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

    if (!fetchedEntity) {
        return (
            <Tooltip title={t('mentions.notFound')}>
                <InvalidEntityText>{name}</InvalidEntityText>
            </Tooltip>
        );
    }

    const entity = fetchedEntity as Entity;
    const displayName = registry.getDisplayName(entity.type, entity);

    return (
        <HoverEntityTooltip entity={entity} card={<MentionHoverCard entity={entity} />}>
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
