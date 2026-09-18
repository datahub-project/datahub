import type { Icon } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components/macro';

const Container = styled.div<{ $color: string; $size: number; $radius: number }>`
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: ${(props) => props.$radius}px;
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
    min-width: ${(props) => props.$size}px;
    color: ${(props) => `color-mix(in srgb, ${props.$color} 75%, ${props.theme.colors.text})`};
    background-color: ${(props) => `color-mix(in srgb, ${props.$color} 12%, ${props.theme.colors.bg})`};
    flex-shrink: 0;
`;

interface Props {
    /** Entity accent color. Tinted against the surface for the fill and the glyph. */
    color: string;
    icon: Icon;
    size?: number;
    iconSize?: number;
    /** Override the container's border-radius (defaults to `size / 4`). */
    radius?: number;
    className?: string;
}

/**
 * Tinted, rounded-square icon container shared by the entity types that carry their own color
 * rather than a platform logo (domains, glossary terms and nodes, tags). Keeping the geometry and
 * the color-mix ratios in one place is what makes those icons look like a set wherever they're
 * rendered side by side — hover cards, autocomplete rows, sidebars.
 */
export default function ColoredEntityIcon({
    color,
    icon: IconComponent,
    size = 24,
    iconSize,
    radius,
    className,
}: Props) {
    const resolvedIconSize = iconSize ?? Math.round(size * 0.6);
    const resolvedRadius = radius ?? size / 4;

    return (
        <Container $color={color} $size={size} $radius={resolvedRadius} className={className}>
            <IconComponent size={resolvedIconSize} color="currentColor" weight="bold" />
        </Container>
    );
}
