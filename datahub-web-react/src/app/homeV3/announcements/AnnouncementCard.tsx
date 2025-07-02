import { Card, Icon, Text, colors } from '@components';
import React from 'react';

import { Post } from '@types';

const cardStyles = {
    backgroundColor: colors.violet[0],
    padding: '8px',
    border: 'none',
};

interface Props {
    announcement: Post;
}

export const AnnouncementCard = ({ announcement }: Props) => {
    return (
        <Card
            icon={<Icon icon="MegaphoneSimple" source="phosphor" color="violet" weight="fill" size="2xl" />}
            title={
                <Text color="violet" weight="semiBold" size="md" lineHeight="normal">
                    {announcement.content.title}
                </Text>
            }
            subTitle={
                <Text color="violet" lineHeight="normal">
                    {announcement.content.description || ''}
                </Text>
            }
            button={<Icon icon="X" source="phosphor" color="violet" size="xl" />}
            width="100%"
            style={cardStyles}
        />
    );
};
