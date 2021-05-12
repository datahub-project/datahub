import { Button, Divider, Row, Space, Typography } from 'antd';
import React from 'react';
import { AuditStamp, Ownership, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import { AvatarsGroup } from '../../../shared/avatar';
import analytics, { EventType, EntityActionType } from '../../../analytics';

const styles = {
    content: { width: '100%' },
};

export type Props = {
    urn: string;
    platform: string;
    description?: string | null;
    ownership?: Ownership | null;
    lastModified?: AuditStamp;
    externalUrl?: string | null;
};

export default function DashboardHeader({ urn, platform, description, ownership, externalUrl, lastModified }: Props) {
    const entityRegistry = useEntityRegistry();
    const capitalizedPlatform = capitalizeFirstLetter(platform);

    const openExternalUrl = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType: EntityType.Dashboard,
            entityUrn: urn,
        });
        window.open(externalUrl || undefined, '_blank');
    };

    return (
        <Space direction="vertical" size={16} style={styles.content}>
            <Row justify="space-between">
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text type="secondary">Dashboard</Typography.Text>
                    <Typography.Text strong type="secondary">
                        {capitalizedPlatform}
                    </Typography.Text>
                    {externalUrl && <Button onClick={openExternalUrl}>View in {capitalizedPlatform}</Button>}
                </Space>
            </Row>
            <Typography.Paragraph>{description}</Typography.Paragraph>
            <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} size="large" />
            {lastModified && (
                <Typography.Text type="secondary">
                    Last modified at {new Date(lastModified.time).toLocaleDateString('en-US')}
                </Typography.Text>
            )}
        </Space>
    );
}
