import { Avatar, Button, Divider, Row, Space, Tooltip, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { AuditStamp, EntityType, Ownership } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import defaultAvatar from '../../../../images/default_avatar.png';

const styles = {
    content: { width: '100%' },
    typeLabel: { color: 'rgba(0, 0, 0, 0.45)' },
    platformLabel: { color: 'rgba(0, 0, 0, 0.45)' },
    lastUpdatedLabel: { color: 'rgba(0, 0, 0, 0.45)' },
};

export type Props = {
    platform: string;
    description?: string;
    ownership?: Ownership | null;
    lastModified?: AuditStamp;
    url?: string | null;
};

export default function ChartHeader({ platform, description, ownership, url, lastModified }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <Space direction="vertical" size={15} style={styles.content}>
            <Row justify="space-between">
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text style={styles.typeLabel}>Chart</Typography.Text>
                    <Typography.Text strong style={styles.platformLabel}>
                        {platform}
                    </Typography.Text>
                </Space>
                {url && <Button href={url}>View in {platform}</Button>}
            </Row>
            <Typography.Paragraph>{description}</Typography.Paragraph>
            <Avatar.Group maxCount={6} size="large">
                {ownership?.owners?.map((owner: any) => (
                    <Tooltip title={owner.owner.info?.fullName}>
                        <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${owner.owner.urn}`}>
                            <Avatar src={owner.owner.editableInfo.pictureLink || defaultAvatar} />
                        </Link>
                    </Tooltip>
                ))}
            </Avatar.Group>
            {lastModified && (
                <Typography.Text style={styles.lastUpdatedLabel}>
                    Last modified at {new Date(lastModified.time).toLocaleDateString('en-US')}
                </Typography.Text>
            )}
        </Space>
    );
}
