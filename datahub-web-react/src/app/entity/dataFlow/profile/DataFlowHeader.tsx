import { Button, Divider, Row, Space, Typography } from 'antd';
import React from 'react';
import { DataFlow, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import { AvatarsGroup } from '../../../shared/avatar';
import analytics, { EventType, EntityActionType } from '../../../analytics';

export type Props = {
    dataFlow: DataFlow;
};

export default function DataFlowHeader({ dataFlow: { urn, ownership, info, orchestrator } }: Props) {
    const entityRegistry = useEntityRegistry();
    const platformName = capitalizeFirstLetter(orchestrator);

    const openExternalUrl = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType: EntityType.DataFlow,
            entityUrn: urn,
        });
        window.open(info?.externalUrl || undefined, '_blank');
    };

    return (
        <>
            <Space direction="vertical" size="middle">
                <Row justify="space-between">
                    <Space split={<Divider type="vertical" />}>
                        <Typography.Text>Data Pipeline</Typography.Text>
                        <Typography.Text strong>{platformName}</Typography.Text>
                        {info?.externalUrl && <Button onClick={openExternalUrl}>View in {platformName}</Button>}
                    </Space>
                </Row>
                <Typography.Paragraph>{info?.description}</Typography.Paragraph>
                <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} size="large" />
            </Space>
        </>
    );
}
