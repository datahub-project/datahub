import { List, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { DownstreamEntityRelationships, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

const styles = {
    list: { marginTop: '12px', padding: '16px 32px' },
};

const DashboardItem = styled(List.Item)`
    padding-top: 20px;
`;

export type Props = {
    downstreamLineage?: DownstreamEntityRelationships | null;
};

export default function DashboardCharts({ downstreamLineage }: Props) {
    const entityRegistry = useEntityRegistry();
    const downstreamEntities =
        downstreamLineage?.entities?.map((entityRelationship) => entityRelationship?.entity) || [];

    return (
        <List
            style={styles.list}
            bordered
            dataSource={downstreamEntities}
            header={<Typography.Title level={3}>Dashboards</Typography.Title>}
            renderItem={(item) => (
                <DashboardItem>
                    {entityRegistry.renderPreview(item?.type || EntityType.Dataset, PreviewType.PREVIEW, item)}
                </DashboardItem>
            )}
        />
    );
}
