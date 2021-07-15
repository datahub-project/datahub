import { List, Typography } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { MlFeature, MlPrimaryKey, Dataset, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

export type Props = {
    features?: Array<MlFeature | MlPrimaryKey>;
};

const ViewRawButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

export default function SourcesView({ features }: Props) {
    const entityRegistry = useEntityRegistry();

    const sources = useMemo(
        () =>
            features?.reduce((accumulator: Array<Dataset>, feature: MlFeature | MlPrimaryKey) => {
                if (feature.__typename === 'MLFeature' && feature.featureProperties?.sources) {
                    // eslint-disable-next-line array-callback-return
                    feature.featureProperties?.sources.map((source: Dataset | null) => {
                        if (source && accumulator.findIndex((dataset) => dataset.urn === source?.urn) === -1) {
                            accumulator.push(source);
                        }
                    });
                } else if (feature.__typename === 'MLPrimaryKey' && feature.primaryKeyProperties?.sources) {
                    // eslint-disable-next-line array-callback-return
                    feature.primaryKeyProperties?.sources.map((source: Dataset | null) => {
                        if (source && accumulator.findIndex((dataset) => dataset.urn === source?.urn) === -1) {
                            accumulator.push(source);
                        }
                    });
                }
                return accumulator;
            }, []),
        [features],
    );

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
                        {entityRegistry.renderPreview(item?.type || EntityType.Dataset, PreviewType.PREVIEW, item)}
                    </List.Item>
                )}
            />
        </>
    );
}
