import * as Muicon from '@mui/icons-material';
import React from 'react';
import styled from 'styled-components';
import { hexToRgba } from '@app/sharedV2/colors/colorUtils';
import { Domain } from '../../../../types.generated';
import { generateColor } from '../components/styled/StyledTag';
import { REDESIGN_COLORS } from '../constants';

const DomainIconContainer = styled.div<{ color: string; size: number }>`
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 8px;
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

// looks through the object keys of Muicon and finds the best match for the search string
// returns the icon if found, otherwise returns undefined
function getIcon(search: string): React.ElementType | undefined {
    // If the search string is empty or consists only of whitespace after trimming,
    // return undefined to signify that no valid search string is present.
    if (!search.trim()) return undefined;

    const icon = Object.keys(Muicon).find((key) => key.toLowerCase().includes(search.toLowerCase()));
    return icon ? Muicon[icon] : undefined;
}

export const DomainColoredIcon = ({ iconColor, domain, size = 40, fontSize = 20, onClick }: Props): JSX.Element => {
    const iconName = domain?.displayProperties?.icon?.name || '';
    const MaterialIcon = getIcon(iconName);

    const domainColor = domain?.displayProperties?.colorHex || generateColor.hex(domain?.urn || '');
    const domainBackgroundColor = hexToRgba(iconColor || domainColor, 0.75);

    return (
        <DomainIconContainer color={domainBackgroundColor} size={size} onClick={onClick}>
            {MaterialIcon ? (
                <MaterialIcon style={{ color: `${REDESIGN_COLORS.WHITE}` }} fontSize="large" sx={{ px: 1 }} />
            ) : (
                <DomainCharacterIcon color={`${REDESIGN_COLORS.WHITE}`} $fontSize={fontSize}>
                    {domain?.properties?.name.charAt(0)}
                </DomainCharacterIcon>
            )}
        </DomainIconContainer>
    );
};
