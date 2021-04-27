import React from 'react';
import { Space, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import CustomAvatar from '../../../shared/avatar/CustomAvatar';

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
                <CustomAvatar size={60} photoUrl={photoUrl} name={name} />
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
