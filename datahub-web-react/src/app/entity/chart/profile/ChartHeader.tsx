import { Button, Divider, Row, Space, Typography } from 'antd';
import React from 'react';
import { AuditStamp, ChartType, Ownership, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { AvatarsGroup } from '../../../shared/avatar';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
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
    chartType?: ChartType | null;
};

export default function ChartHeader({
    urn,
    platform,
    description,
    ownership,
    externalUrl,
    lastModified,
    chartType,
}: Props) {
    const entityRegistry = useEntityRegistry();

    const openExternalUrl = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType: EntityType.Chart,
            entityUrn: urn,
        });
        window.open(externalUrl || undefined, '_blank');
    };

    return (
        <Space direction="vertical" size={15} style={styles.content}>
            <Row justify="space-between">
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text strong type="secondary">
                        {chartType ? `${capitalizeFirstLetter(chartType.toLowerCase())} ` : ''}Chart
                    </Typography.Text>
                    <Typography.Text strong type="secondary">
                        {capitalizeFirstLetter(platform.toLowerCase())}
                    </Typography.Text>
                    {externalUrl && (
                        <Button onClick={openExternalUrl}>View in {capitalizeFirstLetter(platform)}</Button>
                    )}
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
