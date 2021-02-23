import React from 'react';
import { Avatar, Space, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import defaultAvatar from '../../../../images/default_avatar.png';

const styles = {
    name: { margin: 0, color: '#0073b1' },
    title: { color: 'rgba(0, 0, 0, 0.45)' },
};

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
        <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${urn}`}>
            <Space size={28}>
                <Avatar size={60} src={photoUrl || defaultAvatar} />
                <Space direction="vertical" size={4}>
                    <Typography.Title style={styles.name} level={3}>
                        {name}
                    </Typography.Title>
                    <Typography.Text style={styles.title}>{title}</Typography.Text>
                </Space>
            </Space>
        </Link>
    );
};
