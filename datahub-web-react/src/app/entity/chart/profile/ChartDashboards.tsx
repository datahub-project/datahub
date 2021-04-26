import { List, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { DownstreamEntityRelationships, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

const DashboardList = styled(List)`
    margin-top: 12px;
    padding: 16px 32px;
`;
const DashboardItem = styled(List.Item)`
    padding-top: 20px;
`;

export type Props = {
    downstreamLineage?: DownstreamEntityRelationships | null;
};

export default function DashboardCharts({ downstreamLineage }: Props) {
    const entityRegistry = useEntityRegistry();
    const downstreamEntities =
        downstreamLineage?.entities
            ?.map((entityRelationship) => entityRelationship?.entity)
            .filter((item) => item?.type === EntityType.Dashboard) || [];

    return (
        <DashboardList
            bordered
            dataSource={downstreamEntities}
            header={<Typography.Title level={3}>Dashboards</Typography.Title>}
            renderItem={(item) => (
                <DashboardItem>
                    {entityRegistry.renderPreview(EntityType.Dashboard, PreviewType.PREVIEW, item)}
                </DashboardItem>
            )}
        />
    );
}
