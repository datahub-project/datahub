import type { Icon } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components/macro';

import { hexToRgb } from '@app/sharedV2/colors/colorUtils';
import { getLighterRGBColor } from '@app/sharedV2/icons/colorUtils';

const Container = styled.div<{ $bg: string; $size: number }>`
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: ${(props) => props.$size / 4}px;
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
    min-width: ${(props) => props.$size}px;
    background-color: ${(props) => props.$bg};
    flex-shrink: 0;
`;

interface Props {
    color: string;
    icon: Icon;
    size?: number;
    iconSize?: number;
    className?: string;
}

export default function GlossaryColoredIcon({ color, icon: IconComponent, size = 24, iconSize, className }: Props) {
    const [r, g, b] = hexToRgb(color);
    const bg = `rgb(${getLighterRGBColor(r, g, b).join(', ')})`;
    const resolvedIconSize = iconSize ?? Math.round(size * 0.6);

    return (
        <Container $bg={bg} $size={size} className={className}>
            <IconComponent size={resolvedIconSize} color={color} weight="regular" />
        </Container>
    );
}
