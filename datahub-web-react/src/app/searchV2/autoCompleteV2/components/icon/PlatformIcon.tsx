import React, { useState } from 'react';
import styled from 'styled-components';

import { getPlatformName } from '@src/app/entityV2/shared/utils';
import { DataPlatform } from '@src/types.generated';

const ImageIcon = styled.img<{ $size: number }>`
    vertical-align: middle;
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
    object-fit: contain;
    background-color: transparent;
`;

interface Props {
    platform: DataPlatform;
    size: number;
}

export function PlatformIcon({ platform, size }: Props) {
    const [isBrokenPlatformLogoUrl, setIsBrokenPlatformLogoUrl] = useState<boolean>(false);
    const platformLogoUrl = platform?.properties?.logoUrl;
    const platformName = getPlatformName(platform) || '';

    return platformLogoUrl && !isBrokenPlatformLogoUrl ? (
        <ImageIcon
            src={platformLogoUrl}
            alt={platformName}
            $size={size}
            onError={() => setIsBrokenPlatformLogoUrl(true)}
        />
    ) : null;
}
