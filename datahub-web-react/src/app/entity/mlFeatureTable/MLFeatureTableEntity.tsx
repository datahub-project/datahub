import * as React from 'react';
import { DotChartOutlined } from '@ant-design/icons';
import { MlFeatureTable, EntityType, SearchResult, MlFeature, MlPrimaryKey, Dataset } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { MLFeatureTableProfile } from './profile/MLFeatureTableProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { notEmpty } from '../shared/utils';

/**
 * Definition of the DataHub MLFeatureTable entity.
 */
export class MLFeatureTableEntity implements Entity<MlFeatureTable> {
    type: EntityType = EntityType.MlfeatureTable;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DotChartOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DotChartOutlined style={{ fontSize, color: '#9633b9' }} />;
        }

        return (
            <DotChartOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'featureTables';

    getCollectionName = () => 'Feature tables';

    renderProfile = (urn: string) => <MLFeatureTableProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: MlFeatureTable) => {
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                description={data.description}
                owners={data.ownership?.owners}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlFeatureTable;
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                description={data.description || ''}
                owners={data.ownership?.owners}
            />
        );
    };

    getLineageVizConfig = (entity: MlFeatureTable) => {
        const features: Array<MlFeature | MlPrimaryKey> =
            entity.featureTableProperties &&
            (entity.featureTableProperties?.mlFeatures || entity.featureTableProperties?.mlPrimaryKeys)
                ? [
                      ...(entity.featureTableProperties?.mlPrimaryKeys || []),
                      ...(entity.featureTableProperties?.mlFeatures || []),
                  ].filter(notEmpty)
                : [];

        const sources = features?.reduce((accumulator: Array<Dataset>, feature: MlFeature | MlPrimaryKey) => {
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
        }, []);

        console.log('sources', sources);

        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.MlfeatureTable,
            upstreamChildren: sources.map((source) => source.urn),
            downstreamChildren: [],
            icon: entity.platform.info?.logoUrl || undefined,
            platform: entity.platform.name,
        };
    };
}
