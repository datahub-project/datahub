import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { useGetSchemaFieldQuery } from '@graphql/schemaField.generated';
import * as React from 'react';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { downgradeV2FieldPath } from '@app/lineageV2/lineageUtils';
import { decodeSchemaField } from '@app/lineage/utils/columnLineageUtils';
import { PartitionOutlined, PicCenterOutlined } from '@ant-design/icons';
import { EntityType, SchemaFieldEntity as SchemaField, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { Preview } from './preview/Preview';

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

    getPathName = () => 'schemaField';

    getEntityName = () => 'Schema Field';

    getCollectionName = () => 'Schema Fields';

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.SchemaField}
            useEntityQuery={useGetSchemaFieldQuery}
            tabs={[
                {
                    name: 'Lineage',
                    component: LineageTab,
                    icon: PartitionOutlined,
                },
            ]}
            sidebarSections={this.getSidebarSections()}
        />
    );

    getSidebarSections = () => [{ component: SidebarEntityHeader }];

    getGraphName = () => 'schemaField';

    renderPreview = (previewType: PreviewType, data: SchemaField) => (
        <Preview previewType={previewType} datasetUrn={data.parent.urn} name={data.fieldPath} />
    );

    renderSearch = (result: SearchResult) => this.renderPreview(PreviewType.SEARCH, result.entity as SchemaField);

    displayName = (data: SchemaField) => decodeSchemaField(downgradeV2FieldPath(data?.fieldPath) || '') || data.urn;

    getGenericEntityProperties = (data: SchemaField) =>
        getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: (newData) => newData,
        });

    supportedCapabilities = () => new Set([]);
}
