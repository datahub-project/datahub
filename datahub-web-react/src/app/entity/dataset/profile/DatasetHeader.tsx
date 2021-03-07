import { Avatar, Badge, Divider, Popover, Space, Tooltip, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { Dataset, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import defaultAvatar from '../../../../images/default_avatar.png';

export type Props = {
    dataset: Dataset;
};

export default function DatasetHeader({ dataset: { description, ownership, deprecation, platform } }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <>
            <Space direction="vertical" size="middle">
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text style={{ color: 'grey' }}>Dataset</Typography.Text>
                    <Typography.Text strong style={{ color: '#214F55' }}>
                        {platform?.name}
                    </Typography.Text>
                </Space>
                <Typography.Paragraph>{description}</Typography.Paragraph>
                <Avatar.Group maxCount={6} size="large">
                    {ownership?.owners?.map((owner) => (
                        <Tooltip title={owner.owner.info?.fullName} key={owner.owner.urn}>
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
                <div>
                    {deprecation?.deprecated && (
                        <Popover
                            placement="bottomLeft"
                            content={
                                <>
                                    <Typography.Paragraph>By: {deprecation?.actor}</Typography.Paragraph>
                                    {deprecation.decommissionTime && (
                                        <Typography.Paragraph>
                                            On: {new Date(deprecation?.decommissionTime).toUTCString()}
                                        </Typography.Paragraph>
                                    )}
                                    {deprecation?.note && (
                                        <Typography.Paragraph>{deprecation.note}</Typography.Paragraph>
                                    )}
                                </>
                            }
                            title="Deprecated"
                        >
                            <Badge count="Deprecated" />
                        </Popover>
                    )}
                </div>
            </Space>
        </>
    );
}
