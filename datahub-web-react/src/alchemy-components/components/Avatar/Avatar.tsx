import React, { useState } from 'react';

import { AvatarImage, AvatarImageWrapper, AvatarText, Container } from '@components/components/Avatar/components';
import { AvatarProps } from '@components/components/Avatar/types';
import getAvatarColor, { getNameInitials } from '@components/components/Avatar/utils';
import { Icon } from '@components/components/Icon';

export const avatarDefaults: AvatarProps = {
    name: 'User name',
    size: 'default',
    showInPill: false,
    isOutlined: false,
    isGroup: false,
};

export const Avatar = ({
    name = avatarDefaults.name,
    imageUrl,
    size = avatarDefaults.size,
    onClick,
    showInPill = avatarDefaults.showInPill,
    isOutlined = avatarDefaults.isOutlined,
    isGroup = avatarDefaults.isGroup,
}: AvatarProps) => {
    const [hasError, setHasError] = useState(false);

    return (
        <Container onClick={onClick} $hasOnClick={!!onClick} $showInPill={showInPill}>
            {(!isGroup || imageUrl) && (
                <AvatarImageWrapper
                    $color={getAvatarColor(name)}
                    $size={size}
                    $isOutlined={isOutlined}
                    $hasImage={!!imageUrl}
                >
                    {!hasError && imageUrl ? (
                        <AvatarImage src={imageUrl} onError={() => setHasError(true)} />
                    ) : (
                        !isGroup && getNameInitials(name)
                    )}
                </AvatarImageWrapper>
            )}
            {isGroup && !imageUrl && (
                <Icon icon="UsersThree" source="phosphor" variant="filled" color="gray" size="lg" />
            )}
            {showInPill && <AvatarText $size={size}>{name}</AvatarText>}
        </Container>
    );
};
