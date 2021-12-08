import * as React from 'react';
import { DotChartOutlined } from '@ant-design/icons';
import { MlFeatureTable, EntityType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { MLFeatureTableProfile } from './profile/MLFeatureTableProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';

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

    getEntityName = () => 'Feature Table';

    getCollectionName = () => 'Feature Tables';

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
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.MlfeatureTable,
            upstreamChildren: [],
            downstreamChildren: [],
            icon: entity.platform.info?.logoUrl || undefined,
            platform: entity.platform.name,
        };
    };

    displayName = (data: MlFeatureTable) => {
        return data.name;
    };

    getGenericEntityProperties = (mlFeatureTable: MlFeatureTable) => {
        return getDataForEntityType({
            data: mlFeatureTable,
            entityType: this.type,
            getOverrideProperties: (data) => data,
        });
    };
}
