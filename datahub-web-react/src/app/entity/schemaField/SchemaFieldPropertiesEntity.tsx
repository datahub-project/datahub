import * as React from 'react';
import { PicCenterOutlined } from '@ant-design/icons';
import { EntityType, SchemaFieldEntity, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { Preview } from './preview/Preview';

export class SchemaFieldPropertiesEntity implements Entity<SchemaFieldEntity> {
    type: EntityType = EntityType.SchemaField;

    icon = (fontSize: number, styleType: IconStyleType, color = '#BFBFBF') => (
        <PicCenterOutlined style={{ fontSize, color }} />
    );

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    // Currently unused.
    getAutoCompleteFieldName = () => 'schemaField';

    // Currently unused.
    getPathName = () => 'schemaField';

    // Currently unused.
    getEntityName = () => 'schemaField';

    // Currently unused.
    getCollectionName = () => 'schemaFields';

    // Currently unused.
    renderProfile = (_: string) => <></>;

    renderPreview = (previewType: PreviewType, data: SchemaFieldEntity) => (
        <Preview previewType={previewType} datasetUrn={data.parent.urn} name={data.fieldPath} />
    );

    renderSearch = (result: SearchResult) => this.renderPreview(PreviewType.SEARCH, result.entity as SchemaFieldEntity);

    displayName = (data: SchemaFieldEntity) => data?.fieldPath || data.urn;

    getGenericEntityProperties = (data: SchemaFieldEntity) =>
        getDataForEntityType({ data, entityType: this.type, getOverrideProperties: (newData) => newData });

    supportedCapabilities = () => new Set([]);
}
