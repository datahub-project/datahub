import { Avatar, Badge, Divider, Popover, Space, Typography } from 'antd';
import React from 'react';
import { Dataset, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import CustomAvatar from '../../../shared/avatar/CustomAvatar';

export type Props = {
    dataset: Dataset;
};

export default function DatasetHeader({ dataset: { description, ownership, deprecation, platform } }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <>
            <Space direction="vertical" size="middle">
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text>Dataset</Typography.Text>
                    <Typography.Text strong>{platform?.name}</Typography.Text>
                </Space>
                <Typography.Paragraph>{description}</Typography.Paragraph>
                <Avatar.Group maxCount={6} size="large">
                    {ownership?.owners?.map((owner) => (
                        <CustomAvatar
                            key={owner.owner.urn}
                            name={owner.owner.info?.fullName || undefined}
                            url={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${owner.owner.urn}`}
                            photoUrl={owner.owner.editableInfo?.pictureLink || undefined}
                        />
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
