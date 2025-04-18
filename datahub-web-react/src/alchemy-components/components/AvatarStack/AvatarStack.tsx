import React from 'react';
import { AvatarContainer, AvatarStackContainer } from './components';
import { Avatar } from '../Avatar';
import { AvatarItemProps, AvatarStackProps } from './types';

export const avatarListDefaults: AvatarStackProps = {
    avatars: [
        { name: 'John Doe', imageUrl: 'https://randomuser.me/api/portraits/men/1.jpg' },
        { name: 'Test User', imageUrl: 'https://robohash.org/sample-profile.png' },
        { name: 'Micky Test', imageUrl: 'https://randomuser.me/api/portraits/women/1.jpg' },
    ],
    size: 'md',
};

export const AvatarStack = ({ avatars, size = 'md' }: AvatarStackProps) => {
    if (avatars?.length === 0) return <div>-</div>;
    const renderAvatarStack = avatars?.map((avatar: AvatarItemProps) => (
        <AvatarContainer key={avatar.name}>
            <Avatar size={size} isOutlined imageUrl={avatar.imageUrl} name={avatar.name} />
        </AvatarContainer>
    ));
    return <AvatarStackContainer>{renderAvatarStack}</AvatarStackContainer>;
};
