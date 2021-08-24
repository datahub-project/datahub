import { Avatar, Divider, Space, Tooltip, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import defaultAvatar from '../../../../images/default_avatar.png';

type Props = {
    definition: string;
    sourceRef: string;
    sourceUrl: string;
    ownership?: any;
};
export default function GlossaryTermHeader({ definition, sourceRef, sourceUrl, ownership }: Props) {
    const entityRegistry = useEntityRegistry();
    return (
        <>
            <Space direction="vertical" size="middle" style={{ marginBottom: '15px' }}>
                <Typography.Paragraph>{definition}</Typography.Paragraph>
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text>Source</Typography.Text>
                    <Typography.Text strong>{sourceRef}</Typography.Text>
                    {sourceUrl && (
                        <a href={decodeURIComponent(sourceUrl)} target="_blank" rel="noreferrer">
                            view source
                        </a>
                    )}
                </Space>
                {ownership && (
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
                )}
            </Space>
        </>
    );
}
