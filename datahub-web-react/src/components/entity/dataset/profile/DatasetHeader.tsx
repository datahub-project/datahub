import { Avatar, Badge, Popover, Space, Tooltip, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { Dataset, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import defaultAvatar from '../../../../images/default_avatar.png';

export type Props = {
    dataset: Dataset;
};

export default function DatasetHeader({ dataset: { description, ownership, deprecation } }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <>
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
