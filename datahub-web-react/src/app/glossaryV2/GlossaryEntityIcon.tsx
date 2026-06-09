import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import React from 'react';

import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';

import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

/**
 * Minimal shape required to render a glossary entity's icon. Accepts anything
 * that carries the urn, type, and the optional fields used for color resolution.
 * `parentNodes` is only consulted for terms (so a term can inherit its parent
 * node's color); nodes resolve from their own `displayProperties` plus a
 * deterministic palette fallback.
 */
type GlossaryEntityLike = Pick<GlossaryTerm | GlossaryNode, 'urn' | 'type' | 'displayProperties' | 'parentNodes'>;

interface Props {
    entity: GlossaryEntityLike;
    size?: number;
    iconSize?: number;
    /** Override the container's border-radius (defaults to `size / 4`). */
    radius?: number;
    className?: string;
}

/**
 * Entity-aware icon for glossary terms and nodes. Resolves the entity's color
 * using the same chain the sidebar and entity header use:
 *   - term: own `displayProperties.colorHex` → root parent's color → palette
 *   - node: own `displayProperties.colorHex` → palette from its urn
 *
 * Pairs with `BookmarkSimple` (term) or `BookmarksSimple` (node), matching the
 * Phosphor icons used by the glossary browser sidebar.
 */
export default function GlossaryEntityIcon({ entity, size = 24, iconSize, radius, className }: Props) {
    const generateColor = useGenerateGlossaryColorFromPalette();
    const isTerm = entity.type === EntityType.GlossaryTerm;
    const color = isTerm
        ? getGlossaryTermColor(entity as Pick<GlossaryTerm, 'urn' | 'parentNodes' | 'displayProperties'>, generateColor)
        : entity.displayProperties?.colorHex || generateColor(entity.urn);

    return (
        <GlossaryColoredIcon
            color={color}
            icon={isTerm ? BookmarkSimple : BookmarksSimple}
            size={size}
            iconSize={iconSize}
            radius={radius}
            className={className}
        />
    );
}
