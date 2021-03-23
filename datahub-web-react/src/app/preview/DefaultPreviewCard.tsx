import { Avatar, Divider, Image, Row, Space, Tag, Tooltip, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { EntityType, GlobalTags } from '../../types.generated';
import defaultAvatar from '../../images/default_avatar.png';
import { useEntityRegistry } from '../useEntityRegistry';
import TagGroup from '../shared/tags/TagGroup';

interface Props {
    name: string;
    logoUrl?: string;
    url: string;
    description: string;
    type?: string;
    platform?: string;
    qualifier?: string | null;
    tags?: GlobalTags;
    owners?: Array<{ urn: string; name?: string; photoUrl?: string }>;
    snippet?: React.ReactNode;
}

const DescriptionParagraph = styled(Typography.Paragraph)`
    &&& {
        margin-bottom: 0px;
        padding-left: 8px;
    }
`;

const styles = {
    row: { width: '100%', marginBottom: '0px' },
    leftColumn: { maxWidth: '75%' },
    rightColumn: { maxWidth: '25%' },
    logoImage: { width: '48px' },
    name: { fontSize: '18px' },
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
    snippet,
}: Props) {
    const entityRegistry = useEntityRegistry();
    return (
        <Row style={styles.row} justify="space-between">
            <Space direction="vertical" align="start" size={28} style={styles.leftColumn}>
                <Link to={url}>
                    <Space direction="horizontal" size={20} align="center">
                        {logoUrl && <Image style={styles.logoImage} src={logoUrl} preview />}
                        <Space direction="vertical" size={8}>
                            <Typography.Text strong style={styles.name}>
                                {name}
                            </Typography.Text>
                            <Space split={<Divider type="vertical" />} size={16}>
                                <Typography.Text>{type}</Typography.Text>
                                <Typography.Text strong>{platform}</Typography.Text>
                                <Tag>{qualifier}</Tag>
                            </Space>
                        </Space>
                    </Space>
                </Link>
                <div>
                    {description.length === 0 ? (
                        <DescriptionParagraph type="secondary">No description</DescriptionParagraph>
                    ) : (
                        <DescriptionParagraph>{description}</DescriptionParagraph>
                    )}
                    {snippet}
                </div>
            </Space>
            <Space direction="vertical" align="end" size={36} style={styles.rightColumn}>
                <Space direction="vertical" size={12}>
                    <Typography.Text strong>Owned By</Typography.Text>
                    <Avatar.Group maxCount={4}>
                        {owners?.map((owner) => (
                            <Tooltip title={owner.name} key={owner.urn}>
                                <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${owner.urn}`}>
                                    <Avatar src={owner.photoUrl || defaultAvatar} />
                                </Link>
                            </Tooltip>
                        ))}
                    </Avatar.Group>
                </Space>
                <TagGroup editableTags={tags} maxShow={3} />
            </Space>
        </Row>
    );
}
