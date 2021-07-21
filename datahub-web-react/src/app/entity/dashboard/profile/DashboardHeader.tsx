import { Button, Divider, Row, Space, Typography } from 'antd';
import React from 'react';
import { FetchResult, MutationFunctionOptions } from '@apollo/client';
import { EntityType, Dashboard } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import { AvatarsGroup } from '../../../shared/avatar';
import UpdatableDescription from '../../shared/UpdatableDescription';
import analytics, { EventType, EntityActionType } from '../../../analytics';

const styles = {
    content: { width: '100%' },
};

export type Props = {
    dashboard: Dashboard;
    updateDashboard: (options?: MutationFunctionOptions<any, any> | undefined) => Promise<FetchResult>;
};

export default function DashboardHeader({
    dashboard: { urn, type, info, tool, ownership, editableProperties },
    updateDashboard,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const capitalizedPlatform = capitalizeFirstLetter(tool);

    const openExternalUrl = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType: EntityType.Dashboard,
            entityUrn: urn,
        });
        window.open(info?.externalUrl || undefined, '_blank');
    };

    return (
        <Space direction="vertical" size={16} style={styles.content}>
            <Row justify="space-between">
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text type="secondary">Dashboard</Typography.Text>
                    <Typography.Text strong type="secondary">
                        {capitalizedPlatform}
                    </Typography.Text>
                    {info?.externalUrl && <Button onClick={openExternalUrl}>View in {capitalizedPlatform}</Button>}
                </Space>
            </Row>
            <UpdatableDescription
                updateEntity={updateDashboard}
                updatedDescription={editableProperties?.description}
                originalDescription={info?.description}
                entityType={type}
                urn={urn}
            />
            <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} size="large" />
            {info?.lastModified ? (
                <Typography.Text type="secondary">
                    Last modified at {new Date(info?.lastModified.time).toLocaleDateString('en-US')}
                </Typography.Text>
            ) : (
                ''
            )}
        </Space>
    );
}
