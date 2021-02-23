import { Avatar, Button, Row, Space, Tooltip, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { AuditStamp, EntityType, Ownership } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import defaultAvatar from '../../../../images/default_avatar.png';

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
        <>
            <Row justify="space-between">
                <Typography.Title level={5} style={{ color: 'gray' }}>
                    {platform}
                </Typography.Title>
                {url && <Button href={url}>View in {platform}</Button>}
            </Row>
            <Typography.Paragraph>{description}</Typography.Paragraph>
            <Space direction="vertical">
                <Avatar.Group maxCount={6} size="large">
                    {ownership &&
                        ownership.owners &&
                        ownership.owners.map((owner: any) => (
                            <Tooltip title={owner.owner.info?.fullName}>
                                <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${owner.owner.urn}`}>
                                    <Avatar
                                        style={{
                                            color: '#f56a00',
                                            backgroundColor: '#fde3cf',
                                        }}
                                        src={
                                            (owner.owner.editableInfo && owner.owner.editableInfo.pictureLink) ||
                                            defaultAvatar
                                        }
                                    />
                                </Link>
                            </Tooltip>
                        ))}
                </Avatar.Group>
                {lastModified && <div>Last modified at {new Date(lastModified.time).toLocaleDateString('en-US')}</div>}
            </Space>
        </>
    );
}
