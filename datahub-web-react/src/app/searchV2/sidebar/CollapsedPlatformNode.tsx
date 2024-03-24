import React, { useState } from 'react';
import styled from 'styled-components';
import { DatabaseOutlined as DatabaseIcon } from '@ant-design/icons';
import { DataPlatform, EntityType } from '../../../types.generated';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';
import PlatformIcon from '../../sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '../../useEntityRegistry';

export const PlatformIconContainer = styled.div<{ size?: number }>`
    display: flex;
    justify-content: center;

    background-color: transparent;
    padding: 10px 0;

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
    const entityRegistry = useEntityRegistry();
    const label = entityRegistry.getDisplayName(EntityType.DataPlatform, platform);
    const [brokenImage, setBrokenImage] = useState(false);
    const size = 24;

    return (
        <PlatformIconContainer onClick={onClick}>
            {brokenImage && <DatabaseOutlined title={label} size={size} />}
            {!brokenImage && (
                <PlatformIcon
                    platform={platform}
                    size={size}
                    color={REDESIGN_COLORS.BORDER_1}
                    title={label}
                    onError={() => setBrokenImage(true)}
                />
            )}
        </PlatformIconContainer>
    );
};

export default CollapsedPlatformNode;
