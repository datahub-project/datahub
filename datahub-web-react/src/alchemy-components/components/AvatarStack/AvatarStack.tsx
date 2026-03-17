import React from 'react';

import { Avatar } from '@components/components/Avatar';
import { AvatarContainer, AvatarStackContainer } from '@components/components/AvatarStack/components';
import { AvatarItemProps, AvatarStackProps } from '@components/components/AvatarStack/types';

export const AvatarStack = ({
    avatars,
    size = 'md',
    showRemainingNumber = true,
    maxToShow = 4,
    totalCount,
}: AvatarStackProps) => {
    if (!avatars?.length) return <div>-</div>;
    const remainingNumber = (totalCount ?? avatars.length) - maxToShow;
    const renderAvatarStack = avatars?.slice(0, maxToShow).map((avatar: AvatarItemProps) => (
        <AvatarContainer key={avatar.name}>
            <Avatar size={size} isOutlined imageUrl={avatar.imageUrl} name={avatar.name} type={avatar.type} />
        </AvatarContainer>
    ));
    return (
        <AvatarStackContainer>
            {renderAvatarStack}
            {showRemainingNumber && remainingNumber > 0 && (
                <AvatarContainer key="more-avatars">
                    <Avatar size={size} isOutlined name={`+${remainingNumber}`} />
                </AvatarContainer>
            )}
        </AvatarStackContainer>
    );
};
