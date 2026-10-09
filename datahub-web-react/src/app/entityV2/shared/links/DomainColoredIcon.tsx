import React from 'react';
import styled from 'styled-components';

import { getLazyIcon } from '@app/mfeframework/lazyIconRegistry';
import { useGenerateDomainColorFromPalette } from '@app/sharedV2/colors/colorUtils';
import { coloredIconBackground, coloredIconForeground } from '@app/sharedV2/icons/coloredIconMix';
import { resolveDisplayIconName } from '@app/sharedV2/icons/resolveDisplayIcon';

import { Domain } from '@types';

const DomainIconContainer = styled.div<{ $color: string; size: number }>`
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: ${(props) => props.size / 4}px;
    height: ${(props) => props.size}px;
    width: ${(props) => props.size}px;
    min-width: ${(props) => props.size}px;
    color: ${(props) => coloredIconForeground(props.$color, props.theme.colors.text)};
    background-color: ${(props) => coloredIconBackground(props.$color, props.theme.colors.bg)};
`;

const DomainCharacterIcon = styled.div<{ $fontSize: number }>`
    font-size: ${(props) => (props.$fontSize ? props.$fontSize : '20')}px;
    font-weight: 600;
`;

type Props = {
    iconColor?: string;
    domain: Domain;
    size?: number;
    fontSize?: number;
    onClick?: () => void;
};

export const DomainColoredIcon = ({ iconColor, domain, size = 40, fontSize = 20, onClick }: Props): JSX.Element => {
    const phosphorName = resolveDisplayIconName(
        domain?.displayProperties?.icon?.name,
        domain?.displayProperties?.icon?.iconLibrary,
    );

    const generateColor = useGenerateDomainColorFromPalette();
    const domainColor = domain?.displayProperties?.colorHex || generateColor(domain?.urn || '');

    const domainHexColor = iconColor || domainColor;

    return (
        <DomainIconContainer $color={domainHexColor} size={size} onClick={onClick}>
            {phosphorName ? (
                getLazyIcon(phosphorName, { size: fontSize, color: 'currentColor' })
            ) : (
                <DomainCharacterIcon $fontSize={fontSize}>{domain?.properties?.name?.charAt(0)}</DomainCharacterIcon>
            )}
        </DomainIconContainer>
    );
};
