import React from 'react';
import styled from 'styled-components';
import { DataPlatform } from '../../../types.generated';

const Icon = styled.img`
    height: 1em;
    width: 1em;
`;

interface Props {
    platform?: DataPlatform | null;
    className?: string;
}

export default function PlatformIcon({ platform, className }: Props): JSX.Element | null {
    if (!platform?.properties?.logoUrl) {
        return null;
    }
    return (
        <Icon
            src={platform.properties?.logoUrl}
            alt={platform.properties?.displayName || platform.name}
            className={className}
        />
    );
}
