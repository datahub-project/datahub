import React from 'react';
import styled from 'styled-components';

import { resolveDomainEntityColor, resolveDomainIconDisplay } from '@app/entityV2/domain/utils/displayProperties';
import { getLazyIcon } from '@app/mfeframework/lazyIconRegistry';
import { hexToRgb, hexToRgba, useGenerateDomainColorFromPalette } from '@app/sharedV2/colors/colorUtils';
import { getLighterRGBColor } from '@app/sharedV2/icons/colorUtils';

import { Domain } from '@types';

const DomainIconContainer = styled.div<{ color: string; size: number }>`
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: ${(props) => props.size / 4}px;
    height: ${(props) => props.size}px;
    width: ${(props) => props.size}px;
    min-width: ${(props) => props.size}px;
    background-color: ${({ color }) => color};
`;

const DomainCharacterIcon = styled.div<{ color: string; $fontSize: number }>`
    font-size: ${(props) => (props.$fontSize ? props.$fontSize : '20')}px;
    font-weight: 500;
    color: ${({ color }) => color};
`;

type Props = {
    iconColor?: string;
    domain: Domain;
    size?: number;
    fontSize?: number;
    onClick?: () => void;
};

export const DomainColoredIcon = ({ iconColor, domain, size = 40, fontSize = 20, onClick }: Props): JSX.Element => {
    // Read-side backward-compat lives in `resolveDomainIconDisplay` — it maps historical MUI
    // names to Phosphor and guards against rendering the AppWindow fallback for unknown names,
    // returning `showIcon: false` so we fall through to the letter avatar instead.
    const { iconName, showIcon } = resolveDomainIconDisplay(domain?.displayProperties?.icon?.name);

    const generateColor = useGenerateDomainColorFromPalette();
    const domainColor = resolveDomainEntityColor(domain, generateColor);

    const domainHexColor = iconColor || domainColor;
    const [r, g, b] = hexToRgb(domainHexColor);
    const domainBackgroundColor = `rgb(${getLighterRGBColor(r, g, b).join(', ')})`;
    const domainIconColor = hexToRgba(domainHexColor, 1.0);

    // Lazy-load only the specific icon this domain uses — each Phosphor icon lives in its own
    // async chunk (see mfeframework/lazy-icons/), preserving the tree-shake work from PR #16338.
    return (
        <DomainIconContainer color={domainBackgroundColor} size={size} onClick={onClick}>
            {showIcon ? (
                getLazyIcon(iconName, { color: domainIconColor, size: fontSize, weight: 'regular' })
            ) : (
                <DomainCharacterIcon color={domainIconColor} $fontSize={fontSize}>
                    {domain?.properties?.name?.charAt(0) ?? ''}
                </DomainCharacterIcon>
            )}
        </DomainIconContainer>
    );
};
