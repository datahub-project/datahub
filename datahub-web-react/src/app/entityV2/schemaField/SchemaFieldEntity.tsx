import * as React from 'react';
import { PicCenterOutlined } from '@ant-design/icons';
import { EntityType, SchemaFieldEntity as SchemaField, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { Preview } from './preview/Preview';
import { downgradeV2FieldPath } from '../../lineageV2/lineageUtils';
import { decodeSchemaField } from '../../lineage/utils/columnLineageUtils';

export class SchemaFieldEntity implements Entity<SchemaField> {
    type: EntityType = EntityType.SchemaField;

    icon = (fontSize?: number, styleType?: IconStyleType, color = '#BFBFBF') => (
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
    getEntityName = () => 'Schema Field';

    // Currently unused.
    getCollectionName = () => 'Schema Fields';

    // Currently unused.
    renderProfile = (_: string) => <></>;

    getGraphName = () => 'schemaField';

    renderPreview = (previewType: PreviewType, data: SchemaField) => (
        <Preview previewType={previewType} datasetUrn={data.parent.urn} name={data.fieldPath} />
    );

    renderSearch = (result: SearchResult) => this.renderPreview(PreviewType.SEARCH, result.entity as SchemaField);

    displayName = (data: SchemaField) => decodeSchemaField(downgradeV2FieldPath(data?.fieldPath) || '') || data.urn;

    getGenericEntityProperties = (data: SchemaField) =>
        getDataForEntityType({ data, entityType: this.type, getOverrideProperties: (newData) => newData });

    supportedCapabilities = () => new Set([]);
}
