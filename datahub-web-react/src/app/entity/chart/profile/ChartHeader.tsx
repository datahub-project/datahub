import { Button, Divider, Row, Space, Typography } from 'antd';
import React from 'react';
import { FetchResult, MutationFunctionOptions } from '@apollo/client';
import { EntityType, Chart } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { AvatarsGroup } from '../../../shared/avatar';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import UpdatableDescription from '../../shared/UpdatableDescription';
import analytics, { EventType, EntityActionType } from '../../../analytics';

const styles = {
    content: { width: '100%' },
};

export type Props = {
    chart: Chart;
    updateChart: (options?: MutationFunctionOptions<any, any> | undefined) => Promise<FetchResult>;
};

export default function ChartHeader({
    chart: { urn, type, info, tool, ownership, editableProperties },
    updateChart,
}: Props) {
    const entityRegistry = useEntityRegistry();

    const openExternalUrl = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType: EntityType.Chart,
            entityUrn: urn,
        });
        window.open(info?.externalUrl || undefined, '_blank');
    };

    return (
        <Space direction="vertical" size={15} style={styles.content}>
            <Row justify="space-between">
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text strong type="secondary">
                        {info?.type ? `${capitalizeFirstLetter(info?.type.toLowerCase())} ` : ''}Chart
                    </Typography.Text>
                    <Typography.Text strong type="secondary">
                        {capitalizeFirstLetter(tool.toLowerCase())}
                    </Typography.Text>
                    {info?.externalUrl && (
                        <Button onClick={openExternalUrl}>View in {capitalizeFirstLetter(tool)}</Button>
                    )}
                </Space>
            </Row>
            <UpdatableDescription
                updateEntity={updateChart}
                updatedDescription={editableProperties?.description}
                originalDescription={info?.description}
                entityType={type}
                urn={urn}
            />
            <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} size="large" />
            {info?.lastModified?.time && (
                <Typography.Text type="secondary">
                    Last modified at {new Date(info?.lastModified.time).toLocaleDateString('en-US')}
                </Typography.Text>
            )}
        </Space>
    );
}
