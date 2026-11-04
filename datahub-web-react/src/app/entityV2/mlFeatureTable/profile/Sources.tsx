import { EmptyState, Heading } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { PreviewType } from '@app/entityV2/Entity';
import { useBaseEntity } from '@app/entityV2/shared/EntityContext';
import { notEmpty } from '@app/entityV2/shared/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetMlFeatureTableQuery } from '@graphql/mlFeatureTable.generated';
import { Dataset, EntityType } from '@types';

const ViewRawButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

const ListContainer = styled.div`
    padding: 0 32px 16px 32px;
`;

const ListHeader = styled.div`
    padding: 12px 0;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const ListItem = styled.div`
    padding: 20px 0 12px 0;

    &:not(:last-child) {
        border-bottom: 1px solid ${(props) => props.theme.colors.border};
    }
`;

export default function SourcesView() {
    const entityRegistry = useEntityRegistry();
    const { t } = useTranslation('entity.types');
    const { t: tAlchemy } = useTranslation('alchemy');
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
                    feature.properties?.sources?.map((source: Dataset | null) => {
                        if (source && accumulator.findIndex((dataset) => dataset.urn === source?.urn) === -1) {
                            accumulator.push(source);
                        }
                    });
                } else if (feature.__typename === 'MLPrimaryKey' && feature.properties?.sources) {
                    // eslint-disable-next-line array-callback-return
                    feature.properties?.sources?.map((source: Dataset | null) => {
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
            <ListContainer>
                <ListHeader>
                    <Heading type="h3" size="4xl" weight="semiBold">
                        {t('mlFeatureTable.sourcesTab')}
                    </Heading>
                </ListHeader>
                {sources?.length ? (
                    sources.map((item) => (
                        <ListItem key={item.urn}>
                            {entityRegistry.renderPreview(item?.type || EntityType.Dataset, PreviewType.PREVIEW, item)}
                        </ListItem>
                    ))
                ) : (
                    <EmptyState title={tAlchemy('noData')} size="sm" />
                )}
            </ListContainer>
        </>
    );
}
