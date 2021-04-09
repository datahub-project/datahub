import { Avatar, Button, Divider, Row, Space, Typography } from 'antd';
import React from 'react';
import { AuditStamp, EntityType, Ownership } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import CustomAvatar from '../../../shared/avatar/CustomAvatar';

const styles = {
    content: { width: '100%' },
};

export type Props = {
    platform: string;
    description?: string;
    ownership?: Ownership | null;
    lastModified?: AuditStamp;
    url?: string | null;
};

export default function DashboardHeader({ platform, description, ownership, url, lastModified }: Props) {
    const entityRegistry = useEntityRegistry();
    return (
        <Space direction="vertical" size={16} style={styles.content}>
            <Row justify="space-between">
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text type="secondary">Dashboard</Typography.Text>
                    <Typography.Text strong type="secondary">
                        {platform}
                    </Typography.Text>
                </Space>
                {url && <Button href={url}>View in {platform}</Button>}
            </Row>
            <Typography.Paragraph>{description}</Typography.Paragraph>
            <Avatar.Group maxCount={6} size="large">
                {ownership?.owners?.map((owner: any) => (
                    <CustomAvatar
                        key={owner.owner.urn}
                        name={owner.owner.info?.fullName}
                        url={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${owner.owner.urn}`}
                        photoUrl={owner.owner.editableInfo.pictureLink}
                    />
                ))}
            </Avatar.Group>
            {lastModified && (
                <Typography.Text type="secondary">
                    Last modified at {new Date(lastModified.time).toLocaleDateString('en-US')}
                </Typography.Text>
            )}
        </Space>
    );
}
