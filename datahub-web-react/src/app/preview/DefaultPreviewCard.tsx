import { Avatar, Divider, Image, Row, Space, Tag, Tooltip, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { EntityType } from '../../types.generated';
import defaultAvatar from '../../images/default_avatar.png';
import { useEntityRegistry } from '../useEntityRegistry';

interface Props {
    name: string;
    logoUrl?: string;
    url: string;
    description: string;
    type: string;
    platform: string;
    qualifier?: string | null;
    tags: Array<string>;
    owners: Array<{ urn: string; name?: string; photoUrl?: string }>;
}

const styles = {
    row: { width: '100%' },
    leftColumn: { maxWidth: '75%' },
    rightColumn: { maxWidth: '25%' },
    logoImage: { width: '48px' },
    name: { color: '#214F55', fontSize: '18px' },
    typeName: { color: '#585858' },
    platformName: { color: '#585858' },
    ownedBy: { color: '#585858' },
};

export default function DefaultPreviewCard({
    name,
    logoUrl,
    url,
    description,
    type,
    platform,
    qualifier,
    tags,
    owners,
}: Props) {
    const entityRegistry = useEntityRegistry();
    return (
        <Row style={styles.row} justify="space-between">
            <Space direction="vertical" align="start" size={28} style={styles.leftColumn}>
                <Link to={url}>
                    <Space direction="horizontal" size={28} align="center">
                        {logoUrl && <Image style={styles.logoImage} src={logoUrl} preview />}
                        <Space direction="vertical" size={8}>
                            <Typography.Text strong style={styles.name}>
                                {name}
                            </Typography.Text>
                            <Space split={<Divider type="vertical" />} size={16}>
                                <Typography.Text style={styles.typeName}>{type}</Typography.Text>
                                <Typography.Text style={styles.platformName} strong>
                                    {platform}
                                </Typography.Text>
                                <Tag>{qualifier}</Tag>
                            </Space>
                        </Space>
                    </Space>
                </Link>
                <Typography.Paragraph>{description}</Typography.Paragraph>
            </Space>
            <Space direction="vertical" align="end" size={36} style={styles.rightColumn}>
                <Space>
                    {tags.map((tag) => (
                        <Tag color="processing">{tag}</Tag>
                    ))}
                </Space>
                <Space direction="vertical" size={12}>
                    <Typography.Text strong style={styles.ownedBy}>
                        Owned By
                    </Typography.Text>
                    <Avatar.Group maxCount={4}>
                        {owners.map((owner) => (
                            <Tooltip title={owner.name}>
                                <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${owner.urn}`}>
                                    <Avatar src={owner.photoUrl || defaultAvatar} />
                                </Link>
                            </Tooltip>
                        ))}
                    </Avatar.Group>
                </Space>
            </Space>
        </Row>
    );
}
