import * as React from 'react';
import { PicCenterOutlined } from '@ant-design/icons';
import { Dataset, EntityType, SchemaFieldEntity, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { Preview } from './preview/Preview';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';

export class SchemaFieldPropertiesEntity implements Entity<SchemaFieldEntity> {
    type: EntityType = EntityType.SchemaField;

    icon = (fontSize: number, styleType: IconStyleType, color = '#BFBFBF') => (
        <PicCenterOutlined style={{ fontSize, color }} />
    );

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getParentDataset = (parent) => {
        return {
            urn: parent?.urn,
            name: parent?.name,
            type: parent?.type,
            platform: parent?.platfrom,
            properties: parent?.properties,
        } as Dataset;
    };

    // Currently unused.
    getAutoCompleteFieldName = () => 'schemaField';

    // Currently unused.
    getPathName = () => 'schemaField';

    getEntityName = () => 'Column';

    getCollectionName = () => 'Columns';

    // Currently unused.
    renderProfile = (_: string) => <></>;

    getGraphName = () => 'schemaField';

    renderPreview = (previewType: PreviewType, data: SchemaFieldEntity) => {
        const parent = data.parent as Dataset;
        return (
            <Preview
                previewType={previewType}
                datasetUrn={data.parent.urn}
                name={data.fieldPath}
                parentContainers={parent?.parentContainers}
                platformName={
                    parent?.platform?.properties?.displayName || capitalizeFirstLetterOnly(parent?.platform?.name)
                }
                platformLogo={parent?.platform?.properties?.logoUrl || ''}
                platformInstanceId={parent?.dataPlatformInstance?.instanceId}
                parentDataset={this.getParentDataset(parent)}
            />
        );
    };

    renderSearch = (result: SearchResult) => this.renderPreview(PreviewType.SEARCH, result.entity as SchemaFieldEntity);

    displayName = (data: SchemaFieldEntity) => data?.fieldPath || data.urn;

    getGenericEntityProperties = (data: SchemaFieldEntity) =>
        getDataForEntityType({ data, entityType: this.type, getOverrideProperties: (newData) => newData });

    supportedCapabilities = () => new Set([]);
}
