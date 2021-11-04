import { Button, List, Space, Typography } from 'antd';
import React from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';
import { Entity, EntityType } from '../../../../types.generated';
import { navigateToLineageUrl } from '../../../lineage/utils/navigateToLineageUrl';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

export type Props = {
    models?: Entity[];
};

const ViewRawButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

export default function MLGroupModels({ models }: Props) {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const location = useLocation();

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
                    dataSource={models}
                    header={<Typography.Title level={3}>Models</Typography.Title>}
                    renderItem={(item) => (
                        <List.Item style={{ paddingTop: '20px' }}>
                            {entityRegistry.renderPreview(item?.type || EntityType.Mlmodel, PreviewType.PREVIEW, item)}
                        </List.Item>
                    )}
                />
            </Space>
        </>
    );
}
