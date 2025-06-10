import React, { useState } from 'react';

import { AvatarImage, AvatarImageWrapper, AvatarText, Container } from '@components/components/Avatar/components';
import { AvatarProps } from '@components/components/Avatar/types';
import getAvatarColor, { getNameInitials } from '@components/components/Avatar/utils';
import { AvatarType } from '@components/components/AvatarStack/types';
import { Icon } from '@components/components/Icon';

export const avatarDefaults: AvatarProps = {
    name: 'User name',
    size: 'default',
    showInPill: false,
    isOutlined: false,
    type: AvatarType.user,
};

export const Avatar = ({
    name = avatarDefaults.name,
    imageUrl,
    size = avatarDefaults.size,
    onClick,
    type = avatarDefaults.type,
    showInPill = avatarDefaults.showInPill,
    isOutlined = avatarDefaults.isOutlined,
}: AvatarProps) => {
    const [hasError, setHasError] = useState(false);

    const renderAvatarLogo = () => {
        if (type === AvatarType.user) {
            if (!hasError && imageUrl) {
                return <AvatarImage src={imageUrl} onError={() => setHasError(true)} />;
            }
            return <>{getNameInitials(name)}</>;
        }
        if (type === AvatarType.group) {
            return <Icon icon="UsersThree" source="phosphor" size="md" />;
        }
        return null;
    };

    return (
        <Container onClick={onClick} $hasOnClick={!!onClick} $showInPill={showInPill}>
            <AvatarImageWrapper
                $color={getAvatarColor(name)}
                $size={size}
                $isOutlined={isOutlined}
                $hasImage={!!imageUrl}
            >
                {renderAvatarLogo()}
            </AvatarImageWrapper>
            {showInPill && <AvatarText $size={size}>{name}</AvatarText>}
        </Container>
    );
};
