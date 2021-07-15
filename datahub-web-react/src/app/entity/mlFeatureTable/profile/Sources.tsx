import { List, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { MlFeature, MlPrimaryKey, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

export type Props = {
    sources?: Array<MlFeature | MlPrimaryKey>;
};

const ViewRawButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

export default function SourcesView({ sources }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <>
            <div>
                <ViewRawButtonContainer>
                    {
                        // ToDo: uncomment below these after refactored Lineage to support dynamic entities
                        /* <Button onClick={() => navigateToLineageUrl({ location, history, isLineageMode: true })}>
                            View Graph
                        </Button> */
                    }
                </ViewRawButtonContainer>
            </div>
            <List
                style={{ marginTop: '24px', padding: '16px 32px' }}
                bordered
                dataSource={sources}
                header={<Typography.Title level={3}>Sources</Typography.Title>}
                renderItem={(item) => (
                    <List.Item style={{ paddingTop: '20px' }}>
                        {entityRegistry.renderPreview(item?.type || EntityType.Mlfeature, PreviewType.PREVIEW, item)}
                    </List.Item>
                )}
            />
        </>
    );
}
