import React, { useState } from 'react';

import { AvatarImage, AvatarImageWrapper, AvatarText, Container } from '@components/components/Avatar/components';
import { AvatarProps } from '@components/components/Avatar/types';
import getAvatarColor, { getNameInitials } from '@components/components/Avatar/utils';
import { AvatarType } from '@components/components/AvatarStack/types';
import { Icon } from '@components/components/Icon';

import { mapRoleIcon } from '@app/identity/user/UserUtils';

export const avatarDefaults: AvatarProps = {
    name: 'User name',
    size: 'default',
    showInPill: false,
    isOutlined: false,
    type: AvatarType.user,
    pillBorderType: 'default',
};

export const Avatar = ({
    name = avatarDefaults.name,
    imageUrl,
    size = avatarDefaults.size,
    onClick,
    type = avatarDefaults.type,
    showInPill = avatarDefaults.showInPill,
    isOutlined = avatarDefaults.isOutlined,
    extraRightContent,
    pillBorderType = avatarDefaults.pillBorderType,
}: AvatarProps) => {
    const [hasError, setHasError] = useState(false);

    return (
        <Container onClick={onClick} $hasOnClick={!!onClick} $showInPill={showInPill} $borderType={pillBorderType}>
            {(type === AvatarType.user || imageUrl) && (
                <AvatarImageWrapper $color={getAvatarColor(name)} $size={size} $isOutlined={isOutlined}>
                    {!hasError && imageUrl ? (
                        <AvatarImage src={imageUrl} onError={() => setHasError(true)} />
                    ) : (
                        type === AvatarType.user && getNameInitials(name)
                    )}
                </AvatarImageWrapper>
            )}
            {type === AvatarType.group && !imageUrl && (
                <AvatarImageWrapper $color={getAvatarColor(name)} $size={size} $isOutlined={isOutlined}>
                    <Icon icon="UsersThree" source="phosphor" variant="filled" size="lg" />
                </AvatarImageWrapper>
            )}
            {type === AvatarType.role && !imageUrl && (
                <AvatarImageWrapper $color={getAvatarColor(name)} $size={size} $isOutlined={isOutlined}>
                    {mapRoleIcon(name)}
                </AvatarImageWrapper>
            )}
            {showInPill && <AvatarText $size={size}>{name}</AvatarText>}
            {extraRightContent}
        </Container>
    );
};
