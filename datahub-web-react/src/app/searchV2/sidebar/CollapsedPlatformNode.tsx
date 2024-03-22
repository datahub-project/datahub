import React, { useState } from 'react';
import styled from 'styled-components';
import { DatabaseOutlined as DatabaseIcon } from '@ant-design/icons';
import { DataPlatform } from '../../../types.generated';


const PlatformLogo = styled.img<{ size?: number }>`
    max-height: ${(props) => (props.size ? props.size : 12)}px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    margin-top: 12px;
    margin-bottom: 12px;
    :hover {
        cursor: pointer;
    }
`;

const DatabaseOutlined = styled(DatabaseIcon)`
    font-size: 24px;
    cursor: pointer;
    margin-top: 12px;
    margin-bottom: 12px;
`;

type Props = {
    platform: DataPlatform;
    onClick: () => void;
};

const CollapsedPlatformNode = ({ platform, onClick }: Props) => {
    const logoUrl = (platform as DataPlatform)?.properties?.logoUrl;
    const [brokenImage, setBrokenImage] = useState(false);

    const handleImageError = () => {
        setBrokenImage(true);
    };
    if (!logoUrl) {
        return null;
    }

    if (brokenImage) {
        return <DatabaseOutlined title={platform.name} onClick={onClick} />;
    }

    return <PlatformLogo src={logoUrl as string} size={24} onClick={onClick} onError={handleImageError} />;
};

export default CollapsedPlatformNode;
