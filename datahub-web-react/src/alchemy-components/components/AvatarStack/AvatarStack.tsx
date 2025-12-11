/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { Avatar } from '@components/components/Avatar';
import { AvatarContainer, AvatarStackContainer } from '@components/components/AvatarStack/components';
import { AvatarItemProps, AvatarStackProps } from '@components/components/AvatarStack/types';

export const avatarListDefaults: AvatarStackProps = {
    avatars: [
        { name: 'John Doe', imageUrl: 'https://randomuser.me/api/portraits/men/1.jpg' },
        { name: 'Test User', imageUrl: 'https://randomuser.me/api/portraits/men/1.jpg' },
        { name: 'Micky Test', imageUrl: 'https://randomuser.me/api/portraits/men/1.jpg' },
    ],
    size: 'md',
};

export const AvatarStack = ({ avatars, size = 'md', showRemainingNumber = true, maxToShow = 4 }: AvatarStackProps) => {
    if (!avatars?.length) return <div>-</div>;
    const remainingNumber = avatars.length - maxToShow;
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
