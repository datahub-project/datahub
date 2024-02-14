import { List, Typography } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { GetMlFeatureTableQuery } from '../../../../graphql/mlFeatureTable.generated';
import { Dataset, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';
import { useBaseEntity } from '../../shared/EntityContext';
import { notEmpty } from '../../shared/utils';

const ViewRawButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

export default function SourcesView() {
    const entityRegistry = useEntityRegistry();
    const baseEntity = useBaseEntity<GetMlFeatureTableQuery>();
    const featureTable = baseEntity?.mlFeatureTable;

    const features = useMemo(
        () =>
            featureTable?.properties &&
            (featureTable?.properties?.mlFeatures || featureTable?.properties?.mlPrimaryKeys)
                ? [
                      ...(featureTable?.properties?.mlPrimaryKeys || []),
                      ...(featureTable?.properties?.mlFeatures || []),
                  ].filter(notEmpty)
                : [],
        [featureTable?.properties],
    );

    const sources = useMemo(
        () =>
            features?.reduce((accumulator: Array<Dataset>, feature) => {
                if (feature.__typename === 'MLFeature' && feature.properties?.sources) {
                    // eslint-disable-next-line array-callback-return
                    feature.properties?.sources.map((source: Dataset | null) => {
                        if (source && accumulator.findIndex((dataset) => dataset.urn === source?.urn) === -1) {
                            accumulator.push(source);
                        }
                    });
                } else if (feature.__typename === 'MLPrimaryKey' && feature.properties?.sources) {
                    // eslint-disable-next-line array-callback-return
                    feature.properties?.sources.map((source: Dataset | null) => {
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
