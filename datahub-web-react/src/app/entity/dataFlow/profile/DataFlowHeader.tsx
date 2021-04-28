import { Button, Divider, Row, Space, Typography } from 'antd';
import React from 'react';
import { DataFlow } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import { AvatarsGroup } from '../../../shared/avatar';

export type Props = {
    dataFlow: DataFlow;
};

export default function DataFlowHeader({ dataFlow: { ownership, info, orchestrator } }: Props) {
    const entityRegistry = useEntityRegistry();
    const platformName = capitalizeFirstLetter(orchestrator);

    return (
        <>
            <Space direction="vertical" size="middle">
                <Row justify="space-between">
                    <Space split={<Divider type="vertical" />}>
                        <Typography.Text>Data Pipeline</Typography.Text>
                        <Typography.Text strong>{platformName}</Typography.Text>
                        {info?.externalUrl && (
                            <Button onClick={() => window.open(info?.externalUrl || undefined, '_blank')}>
                                View in {platformName}
                            </Button>
                        )}
                    </Space>
                </Row>
                <Typography.Paragraph>{info?.description}</Typography.Paragraph>
                <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} size="large" />
            </Space>
        </>
    );
}
