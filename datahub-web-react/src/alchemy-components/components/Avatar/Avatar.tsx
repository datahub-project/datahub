import React, { useState } from 'react';
import { AvatarImage, AvatarImageWrapper, AvatarText, Container } from './components';
import { AvatarProps } from './types';
import getAvatarColor, { getNameInitials } from './utils';

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
