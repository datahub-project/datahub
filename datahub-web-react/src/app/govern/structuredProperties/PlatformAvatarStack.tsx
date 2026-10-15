import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { Avatar } from '@components/components/Avatar';
import { AvatarContainer, AvatarStackContainer } from '@components/components/AvatarStack/components';

import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { DataPlatform, EntityType } from '@src/types.generated';

const LIST_SEPARATOR = ', ';
const DEFAULT_MAX_TO_SHOW = 4;
// Matches the alchemy Avatar 'md' dimensions so this stack lines up with owner/actor stacks.
const AVATAR_SIZE = 24;
const LOGO_SIZE = 14;

const PlatformAvatar = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: ${AVATAR_SIZE}px;
    height: ${AVATAR_SIZE}px;
    flex-shrink: 0;
    border-radius: 50%;
    border: 1px solid ${(props) => props.theme.colors.border};
    background-color: ${(props) => props.theme.colors.bgSurface};
`;

type Props = {
    platforms: DataPlatform[];
    maxToShow?: number;
};

const PlatformAvatarStack = ({ platforms, maxToShow = DEFAULT_MAX_TO_SHOW }: Props) => {
    const entityRegistry = useEntityRegistry();

    const getPlatformName = (platform: DataPlatform) =>
        entityRegistry.getDisplayName(EntityType.DataPlatform, platform) || platform.urn;

    const remainingCount = platforms.length - maxToShow;

    return (
        <AvatarStackContainer>
            {platforms.slice(0, maxToShow).map((platform) => (
                <AvatarContainer key={platform.urn}>
                    <Tooltip title={getPlatformName(platform)} showArrow={false}>
                        <PlatformAvatar>
                            <PlatformIcon
                                platform={platform}
                                size={LOGO_SIZE}
                                styles={{ padding: 0, background: 'transparent' }}
                            />
                        </PlatformAvatar>
                    </Tooltip>
                </AvatarContainer>
            ))}
            {remainingCount > 0 && (
                <AvatarContainer key="more-platforms">
                    <Tooltip
                        title={platforms.slice(maxToShow).map(getPlatformName).join(LIST_SEPARATOR)}
                        showArrow={false}
                    >
                        <Avatar size="md" isOutlined name={`+${remainingCount}`} />
                    </Tooltip>
                </AvatarContainer>
            )}
        </AvatarStackContainer>
    );
};

export default PlatformAvatarStack;
