import React from 'react';
import { Avatar, Space, Typography } from 'antd';
import { EntityType } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import defaultAvatar from '../../../../images/default_avatar.png';

export const Preview = ({
    urn,
    name,
    title,
    photoUrl,
}: {
    urn: string;
    name: string;
    title?: string;
    photoUrl?: string;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <DefaultPreviewCard
            url={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${urn}`}
            title={
                <Space size="large">
                    <Avatar size="large" src={photoUrl || defaultAvatar} />
                    <Space direction="vertical" size={4}>
                        <Typography.Title style={{ margin: '0', color: '#0073b1' }} level={3}>
                            {name}
                        </Typography.Title>
                        <Typography.Text style={{ color: '#gray' }}>{title}</Typography.Text>
                    </Space>
                </Space>
            }
        />
    );
};
