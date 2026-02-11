import { Icon } from '@components';
import { Image } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { CUSTOM } from '@app/ingestV2/source/builder/constants';
import useGetSourceLogoUrl from '@app/ingestV2/source/builder/useGetSourceLogoUrl';

const PlatformLogo = styled(Image)`
    max-height: 32px;
    height: 32px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    max-width: 32px;
    margin: 8px 0;
`;

const StyledIcon = styled(Icon)`
    margin: 6px 0;
`;

interface Props {
    sourceName: string;
}

export default function SourceLogo({ sourceName }: Props) {
    const logoUrl = useGetSourceLogoUrl(sourceName);

    let logoComponent;
    if (sourceName === CUSTOM) {
        logoComponent = <StyledIcon icon="NotePencil" source="phosphor" color="gray" />;
    }

    return logoUrl ? <PlatformLogo preview={false} src={logoUrl} alt={sourceName} /> : logoComponent || null;
}
