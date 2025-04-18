import React from 'react';
import styled from 'styled-components';

import { Image } from 'antd';

const PlatformLogo = styled(Image)`
    max-height: 35px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

export const ConnectionLogo = ({ logoUrl }: { logoUrl: string }) => {
    return <PlatformLogo src={logoUrl} preview={false} />;
};
