import { Button, List, Space, Typography } from 'antd';
import React from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';
import { DownstreamEntityRelationships, EntityType, UpstreamEntityRelationships } from '../../../../types.generated';
import { navigateToLineageUrl } from '../../../lineage/utils/navigateToLineageUrl';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

export type Props = {
    upstreamLineage?: UpstreamEntityRelationships | null;
    downstreamLineage?: DownstreamEntityRelationships | null;
};

const ViewRawButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

export default function Lineage({ upstreamLineage, downstreamLineage }: Props) {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const location = useLocation();
    const upstreamEntities = upstreamLineage?.entities?.map((entityRelationship) => entityRelationship?.entity);
    const downstreamEntities = downstreamLineage?.entities?.map((entityRelationship) => entityRelationship?.entity);

    return (
        <>
            <div>
                <ViewRawButtonContainer>
                    <Button onClick={() => navigateToLineageUrl({ location, history, isLineageMode: true })}>
                        View Graph
                    </Button>
                </ViewRawButtonContainer>
            </div>
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                <List
                    style={{ marginTop: '24px', padding: '16px 32px' }}
                    bordered
                    dataSource={upstreamEntities}
                    header={<Typography.Title level={3}>Upstream</Typography.Title>}
                    renderItem={(item) => (
                        <List.Item style={{ paddingTop: '20px' }}>
                            {entityRegistry.renderPreview(item?.type || EntityType.Dataset, PreviewType.PREVIEW, item)}
                        </List.Item>
                    )}
                />
                <List
                    style={{ marginTop: '12px', padding: '16px 32px' }}
                    bordered
                    dataSource={downstreamEntities}
                    header={<Typography.Title level={3}>Downstream</Typography.Title>}
                    renderItem={(item) => (
                        <List.Item style={{ paddingTop: '20px' }}>
                            {entityRegistry.renderPreview(item?.type || EntityType.Dataset, PreviewType.PREVIEW, item)}
                        </List.Item>
                    )}
                />
            </Space>
        </>
    );
}
