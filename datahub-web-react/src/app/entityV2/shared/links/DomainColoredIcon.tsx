import React from 'react';
import styled from 'styled-components';

import { hexToRgb, hexToRgba, useGenerateDomainColorFromPalette } from '@app/sharedV2/colors/colorUtils';
import { getLighterRGBColor } from '@app/sharedV2/icons/colorUtils';
import { useMuiIcons } from '@app/sharedV2/icons/useMuiIcons';

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

// looks through the object keys of the icons module and finds the best match for the search string
// returns the icon if found, otherwise returns undefined
function getIcon(search: string, icons: Record<string, React.ElementType>): React.ElementType | undefined {
    if (!search.trim()) return undefined;

    const icon = Object.keys(icons).find((key) => key.toLowerCase().includes(search.toLowerCase()));
    return icon ? icons[icon] : undefined;
}

export const DomainColoredIcon = ({ iconColor, domain, size = 40, fontSize = 20, onClick }: Props): JSX.Element => {
    const icons = useMuiIcons();
    const iconName = domain?.displayProperties?.icon?.name || '';
    const MaterialIcon = icons ? getIcon(iconName, icons) : undefined;

    const generateColor = useGenerateDomainColorFromPalette();
    const domainColor = domain?.displayProperties?.colorHex || generateColor(domain?.urn || '');

    const domainHexColor = iconColor || domainColor;
    const [r, g, b] = hexToRgb(domainHexColor);
    const domainBackgroundColor = `rgb(${getLighterRGBColor(r, g, b).join(', ')})`;
    const domainIconColor = hexToRgba(domainHexColor, 1.0);

    return (
        <DomainIconContainer color={domainBackgroundColor} size={size} onClick={onClick}>
            {MaterialIcon ? (
                <MaterialIcon style={{ color: domainIconColor }} fontSize="large" sx={{ px: 1 }} />
            ) : (
                <DomainCharacterIcon color={domainIconColor} $fontSize={fontSize}>
                    {domain?.properties?.name.charAt(0)}
                </DomainCharacterIcon>
            )}
        </DomainIconContainer>
    );
};
