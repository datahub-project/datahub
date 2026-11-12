import { radius } from '@components';
import { Storefront } from '@phosphor-icons/react/dist/csr/Storefront';
import { Tag as TagIcon } from '@phosphor-icons/react/dist/csr/Tag';
import React, { useMemo } from 'react';
import styled, { useTheme } from 'styled-components';

import { getEntityPlatforms } from '@app/entityV2/shared/containers/profile/header/utils';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import GlossaryEntityIcon from '@app/glossaryV2/GlossaryEntityIcon';
import { PlatformIcon } from '@app/searchV2/autoCompleteV2/components/icon/PlatformIcon';
import { SingleEntityIcon } from '@app/searchV2/autoCompleteV2/components/icon/SingleEntityIcon';
import { EntityIconProps } from '@app/searchV2/autoCompleteV2/components/icon/types';
import useUniqueEntitiesByPlatformUrn from '@app/searchV2/autoCompleteV2/components/icon/useUniqueEntitiesByPlatformUrn';
import ColoredEntityIcon from '@app/sharedV2/icons/ColoredEntityIcon';
import { getTagColor } from '@app/tags/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Domain, EntityType, GlossaryNode, GlossaryTerm } from '@types';

// Platform logos get a circular surface, distinct from the rounded-square tiles used by the
// colored entity types (domains, terms, tags) below.
const Container = styled.div<{ $size: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    background: ${(props) => props.theme.colors.bgSurface};
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
    border-radius: ${radius.full};
    overflow: hidden;
`;

// Entity types that bring their own color render an already-styled tile, so this wrapper only
// reserves the slot — no surface of its own, unlike the platform-logo `Container` above.
const ColoredIconContainer = styled.div<{ $size: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
`;

const IconContainer = styled.div<{ $overlap: number }>`
    // The logo is an antd <Image>, whose inline-block wrapper carries a line box taller than the
    // image itself. That extra leading sits below the glyph, so centring the wrapper leaves the
    // glyph riding high in the circle. Zeroing the line height collapses the leading.
    display: flex;
    align-items: center;
    justify-content: center;
    line-height: 0;

    margin-left: -${(props) => props.$overlap}px;
    &:first-child {
        margin-left: 0;
    }
`;

// Default sizing for callers that don't opt into a custom `size` — kept as separate constants
// (rather than deriving from `size`) so existing autocomplete-row callers render unchanged.
const DEFAULT_CONTAINER_SIZE = 28;
const DEFAULT_ICON_SIZE = 20;
const DEFAULT_SIBLING_ICON_SIZE = 16;
// Inset ratio for a lone platform logo inside its circular surface, matching the app's standard
// platform treatment elsewhere (an 18px logo in a 32px circle). Stacked logos are sized by
// getStackedIconSize instead, so the overlapped group fills the circle.
const PLATFORM_ICON_RATIO = 18 / 32;
const DEFAULT_COLORED_ICON_SIZE = 24;
const DEFAULT_SIBLING_OVERLAP = 4;
const DEFAULT_DOMAIN_FONT_SIZE = 16;
const DEFAULT_GLOSSARY_ICON_SIZE = 14;
/** Fraction of each stacked logo that overlaps the previous one. */
const STACK_OVERLAP_RATIO = 0.3;

/**
 * Size each logo in a stacked pair/group so the overlapped composite still fits `containerSize`.
 * e.g. two logos in a 14px chip → ~8px each with ~2px overlap → 14px total.
 *
 * @param containerSize - Outer slot width/height in px.
 * @param count - Number of logos in the stack.
 * @returns Per-logo size in px.
 */
export function getStackedIconSize(containerSize: number, count: number): number {
    if (count <= 1) return containerSize;
    const factor = 1 + (count - 1) * (1 - STACK_OVERLAP_RATIO);
    return Math.max(8, Math.round(containerSize / factor));
}

/**
 * Overlap between stacked logos, scaled to the per-logo size.
 *
 * @param iconSize - Per-logo size from {@link getStackedIconSize}.
 * @returns Negative margin magnitude in px.
 */
export function getStackedIconOverlap(iconSize: number): number {
    return Math.max(2, Math.round(iconSize * STACK_OVERLAP_RATIO));
}

export default function DefaultEntityIcon({ entity, siblings, size, preferSingleIcon }: EntityIconProps) {
    const entityRegistry = useEntityRegistry();
    const theme = useTheme();
    const uniqueSiblingsByPlatform = useUniqueEntitiesByPlatformUrn(preferSingleIcon ? undefined : siblings);
    const hasSiblings = useMemo(
        () => !preferSingleIcon && (uniqueSiblingsByPlatform?.length ?? 0) > 0,
        [preferSingleIcon, uniqueSiblingsByPlatform?.length],
    );
    const entitiesToShowIcons = useMemo(
        () => (hasSiblings ? uniqueSiblingsByPlatform : [entity]),
        [hasSiblings, uniqueSiblingsByPlatform, entity],
    );
    const containerSize = size ?? DEFAULT_CONTAINER_SIZE;

    const properties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const { platforms } = getEntityPlatforms(entity.type, properties);

    // Domains, glossary entities, tags, and data products all render as a tinted tile sized off the
    // same base, so the glyph keeps its proportions instead of going full-bleed on a caller `size`.
    const coloredIconSize = size ?? DEFAULT_COLORED_ICON_SIZE;
    const coloredGlyphSize = Math.round((coloredIconSize * DEFAULT_GLOSSARY_ICON_SIZE) / DEFAULT_COLORED_ICON_SIZE);

    if (entity.type === EntityType.Domain) {
        const domainFontSize = Math.round((coloredIconSize * DEFAULT_DOMAIN_FONT_SIZE) / DEFAULT_COLORED_ICON_SIZE);
        return (
            <ColoredIconContainer $size={containerSize}>
                <DomainColoredIcon domain={entity as Domain} size={coloredIconSize} fontSize={domainFontSize} />
            </ColoredIconContainer>
        );
    }

    if (entity.type === EntityType.GlossaryTerm || entity.type === EntityType.GlossaryNode) {
        return (
            <ColoredIconContainer $size={containerSize}>
                <GlossaryEntityIcon
                    entity={entity as GlossaryTerm | GlossaryNode}
                    size={coloredIconSize}
                    iconSize={coloredGlyphSize}
                />
            </ColoredIconContainer>
        );
    }

    if (entity.type === EntityType.Tag) {
        return (
            <ColoredIconContainer $size={containerSize}>
                <ColoredEntityIcon
                    color={getTagColor(entity)}
                    icon={TagIcon}
                    size={coloredIconSize}
                    iconSize={coloredGlyphSize}
                />
            </ColoredIconContainer>
        );
    }

    // Data products carry no color of their own (no `displayProperties`), so they get the same tile
    // geometry as domains and terms but tinted neutral rather than inventing an accent for them.
    if (entity.type === EntityType.DataProduct) {
        return (
            <ColoredIconContainer $size={containerSize}>
                <ColoredEntityIcon
                    color={theme.colors.icon}
                    icon={Storefront}
                    size={coloredIconSize}
                    iconSize={coloredGlyphSize}
                />
            </ColoredIconContainer>
        );
    }

    if (!preferSingleIcon && !hasSiblings && (platforms?.length ?? 0) > 1) {
        const platformCount = platforms?.length ?? 0;
        const platformIconSize =
            size !== undefined ? getStackedIconSize(size, platformCount) : DEFAULT_SIBLING_ICON_SIZE;
        const overlap = size !== undefined ? getStackedIconOverlap(platformIconSize) : DEFAULT_SIBLING_OVERLAP;
        return (
            <Container $size={containerSize}>
                {platforms?.map((platform) => (
                    <IconContainer key={platform.urn} $overlap={overlap}>
                        <PlatformIcon platform={platform} size={platformIconSize} />
                    </IconContainer>
                ))}
            </Container>
        );
    }

    const stackCount = entitiesToShowIcons?.length ?? 1;
    let iconSize = DEFAULT_ICON_SIZE;
    if (size !== undefined) {
        // The lone logo is inset in its circular surface rather than filling it — without this, a
        // caller `size` sizes the glyph to the whole container, hiding the background and reading
        // as an oversized bare logo. Defaults are left as-is so autocomplete rows render unchanged.
        iconSize = hasSiblings ? getStackedIconSize(size, stackCount) : Math.round(size * PLATFORM_ICON_RATIO);
    } else if (hasSiblings) {
        iconSize = DEFAULT_SIBLING_ICON_SIZE;
    }
    const overlap = size !== undefined && hasSiblings ? getStackedIconOverlap(iconSize) : DEFAULT_SIBLING_OVERLAP;

    return (
        <Container $size={containerSize}>
            {entitiesToShowIcons?.map((entityToShowIcon) => (
                <IconContainer key={entityToShowIcon.urn} $overlap={overlap}>
                    <SingleEntityIcon entity={entityToShowIcon} size={iconSize} />
                </IconContainer>
            ))}
        </Container>
    );
}
