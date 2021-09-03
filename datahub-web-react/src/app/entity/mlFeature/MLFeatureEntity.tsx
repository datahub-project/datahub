import * as React from 'react';
import { DotChartOutlined } from '@ant-design/icons';
import { MlFeature, EntityType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { MLFeatureProfile } from './profile/MLFeatureProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';

/**
 * Definition of the DataHub MLFeature entity.
 */
export class MLFeatureEntity implements Entity<MlFeature> {
    type: EntityType = EntityType.Mlfeature;

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

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'features';

    getCollectionName = () => 'Features';

    renderProfile = (urn: string) => <MLFeatureProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: MlFeature) => {
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                featureNamespace={data.featureNamespace || ''}
                description={data.description}
                owners={data.ownership?.owners}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlFeature;
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                featureNamespace={data.featureNamespace || ''}
                description={data.description || ''}
                owners={data.ownership?.owners}
            />
        );
    };

    displayName = (data: MlFeature) => {
        return data.name;
    };
}
