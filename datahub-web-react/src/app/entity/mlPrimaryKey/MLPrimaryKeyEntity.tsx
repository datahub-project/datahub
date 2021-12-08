import * as React from 'react';
import { DotChartOutlined } from '@ant-design/icons';
import { MlPrimaryKey, EntityType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { MLPrimaryKeyProfile } from './profile/MLPrimaryKeyProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';

/**
 * Definition of the DataHub MLPrimaryKey entity.
 */
export class MLPrimaryKeyEntity implements Entity<MlPrimaryKey> {
    type: EntityType = EntityType.MlprimaryKey;

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

    getPathName = () => 'mlPrimaryKeys';

    getEntityName = () => 'ML Primary Key';

    getCollectionName = () => 'ML Primary Keys';

    renderProfile = (urn: string) => <MLPrimaryKeyProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: MlPrimaryKey) => {
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
        const data = result.entity as MlPrimaryKey;
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

    displayName = (data: MlPrimaryKey) => {
        return data.name;
    };

    getGenericEntityProperties = (mlPrimaryKey: MlPrimaryKey) => {
        return getDataForEntityType({
            data: mlPrimaryKey,
            entityType: this.type,
            getOverrideProperties: (data) => data,
        });
    };
}
