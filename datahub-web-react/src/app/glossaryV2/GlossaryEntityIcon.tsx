import React from 'react';

import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import {
    GlossaryEntityColorInput,
    resolveGlossaryEntityColor,
    useGenerateGlossaryColorFromPalette,
} from '@app/glossaryV2/colorUtils';
import { getGlossaryEntityIcon } from '@app/glossaryV2/utils';

import { GlossaryNode, GlossaryTerm } from '@types';

/**
 * Minimal shape required to render a glossary entity's icon. Re-exported through
 * `GlossaryEntityColorInput` plus `type`, so the icon component accepts anything that carries the
 * URN, type, and the optional fields used for color resolution.
 */
type GlossaryEntityLike = GlossaryEntityColorInput & Pick<GlossaryTerm | GlossaryNode, 'type'>;

interface Props {
    entity: GlossaryEntityLike;
    size?: number;
    iconSize?: number;
    /** Override the container's border-radius (defaults to `size / 4`). */
    radius?: number;
    className?: string;
}

/**
 * Entity-aware icon for glossary terms and nodes. Routes color through the canonical
 * {@link resolveGlossaryEntityColor} and icon through {@link getGlossaryEntityIcon} so the
 * sidebar, list cards, entity header, and autocomplete all render the same color/icon pair for
 * a given entity.
 */
export default function GlossaryEntityIcon({ entity, size = 24, iconSize, radius, className }: Props) {
    const generateColor = useGenerateGlossaryColorFromPalette();
    const color = resolveGlossaryEntityColor(entity, generateColor);
    const Icon = getGlossaryEntityIcon(entity.type);

    return (
        <GlossaryColoredIcon
            color={color}
            icon={Icon}
            size={size}
            iconSize={iconSize}
            radius={radius}
            className={className}
        />
    );
}
