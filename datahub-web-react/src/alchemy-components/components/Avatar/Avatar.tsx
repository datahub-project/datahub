import React, { useState } from 'react';

import { AvatarImage, AvatarImageWrapper, AvatarText, Container } from '@components/components/Avatar/components';
import { AvatarProps } from '@components/components/Avatar/types';
import getAvatarColor, { getNameInitials } from '@components/components/Avatar/utils';

export const avatarDefaults: AvatarProps = {
    name: 'User name',
    size: 'default',
    showInPill: false,
    isOutlined: false,
};

export const Avatar = ({
    name = avatarDefaults.name,
    imageUrl,
    size = avatarDefaults.size,
    onClick,
    showInPill = avatarDefaults.showInPill,
    isOutlined = avatarDefaults.isOutlined,
}: AvatarProps) => {
    const [hasError, setHasError] = useState(false);

    return (
        <Container onClick={onClick} $hasOnClick={!!onClick} $showInPill={showInPill}>
            <AvatarImageWrapper
                $color={getAvatarColor(name)}
                $size={size}
                $isOutlined={isOutlined}
                $hasImage={!!imageUrl}
            >
                {!hasError && imageUrl ? (
                    <AvatarImage src={imageUrl} onError={() => setHasError(true)} />
                ) : (
                    <>{getNameInitials(name)} </>
                )}
            </AvatarImageWrapper>
            {showInPill && <AvatarText $size={size}>{name}</AvatarText>}
        </Container>
    );
};
