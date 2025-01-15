import React from 'react';
import { AvatarContainer, AvatarListContainer } from './components';
import { Avatar } from '../Avatar';
import { AvatarItemProps, AvatarListProps } from './types';

export const avatarListDefaults: AvatarListProps = {
    avatars: [
        { name: 'John Doe', imageUrl: 'https://randomuser.me/api/portraits/men/1.jpg' },
        { name: 'Test User', imageUrl: 'https://robohash.org/sample-profile.png' },
        { name: 'Micky Test', imageUrl: 'https://randomuser.me/api/portraits/women/1.jpg' },
    ],
    size: 'md',
};

export const AvatarList = ({ avatars, size = 'md' }: AvatarListProps) => {
    if (avatars?.length === 0) return <div>-</div>;
    const renderAvatarList = avatars?.map((avatar: AvatarItemProps) => (
        <AvatarContainer key={avatar.name}>
            <Avatar size={size} isOutlined imageUrl={avatar.imageUrl} name={avatar.name} />
        </AvatarContainer>
    ));
    return <AvatarListContainer>{renderAvatarList}</AvatarListContainer>;
};
