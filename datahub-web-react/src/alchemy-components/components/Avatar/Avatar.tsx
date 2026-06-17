import { UsersThree } from '@phosphor-icons/react/dist/csr/UsersThree';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { AvatarImage, AvatarImageWrapper, AvatarText, Container } from '@components/components/Avatar/components';
import { AvatarProps } from '@components/components/Avatar/types';
import getAvatarColorScheme, { getNameInitials } from '@components/components/Avatar/utils';
import { AvatarType } from '@components/components/AvatarStack/types';
import { Icon } from '@components/components/Icon';

import { mapRoleIcon } from '@app/identity/user/UserUtils';

export const avatarDefaults: AvatarProps = {
    name: 'User name',
    size: 'default',
    showInPill: false,
    isOutlined: false,
    type: AvatarType.user,
};

export const Avatar = ({
    name,
    imageUrl,
    size = avatarDefaults.size,
    onClick,
    type = avatarDefaults.type,
    showInPill = avatarDefaults.showInPill,
    isOutlined = avatarDefaults.isOutlined,
}: AvatarProps) => {
    const { t } = useTranslation('alchemy');
    const resolvedName = name ?? t('avatar.defaultName');
    const [hasError, setHasError] = useState(false);
    const scheme = getAvatarColorScheme(resolvedName);

    return (
        <Container onClick={onClick} $hasOnClick={!!onClick} $showInPill={showInPill}>
            {(type === AvatarType.user || imageUrl) && (
                <AvatarImageWrapper $scheme={scheme} $size={size} $isOutlined={isOutlined}>
                    {!hasError && imageUrl ? (
                        <AvatarImage src={imageUrl} onError={() => setHasError(true)} />
                    ) : (
                        type === AvatarType.user && getNameInitials(resolvedName)
                    )}
                </AvatarImageWrapper>
            )}
            {type === AvatarType.group && !imageUrl && (
                <AvatarImageWrapper $scheme={scheme} $size={size} $isOutlined={isOutlined}>
                    <Icon icon={UsersThree} size="lg" />
                </AvatarImageWrapper>
            )}
            {type === AvatarType.role && !imageUrl && (
                <AvatarImageWrapper $scheme={scheme} $size={size} $isOutlined={isOutlined}>
                    {mapRoleIcon(resolvedName)}
                </AvatarImageWrapper>
            )}
            {showInPill && <AvatarText $size={size}>{resolvedName}</AvatarText>}
        </Container>
    );
};
