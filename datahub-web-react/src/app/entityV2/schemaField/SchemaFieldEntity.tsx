import { GenericEntityProperties } from '@app/entity/shared/types';
import { GlobalEntityRegistryV2 } from '@app/EntityRegistryProvider';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { FetchedEntity } from '@app/lineage/types';
import { useGetSchemaFieldQuery } from '@graphql/schemaField.generated';
import * as React from 'react';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { downgradeV2FieldPath } from '@app/lineageV2/lineageUtils';
import { decodeSchemaField } from '@app/lineage/utils/columnLineageUtils';
import { PartitionOutlined, PicCenterOutlined } from '@ant-design/icons';
import { EntityType, SchemaFieldEntity as SchemaField, SearchResult } from '@types';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { Preview } from './preview/Preview';

export class SchemaFieldEntity implements Entity<SchemaField> {
    type: EntityType = EntityType.SchemaField;

    icon = (fontSize?: number, styleType?: IconStyleType, color = 'inherit') => (
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

    getGenericEntityProperties = (data: SchemaField): GenericEntityProperties | null =>
        getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: (newData) => newData,
        });

    getLineageVizConfig = (entity: SchemaField): FetchedEntity => {
        const parent = GlobalEntityRegistryV2.getGenericEntityProperties(entity.parent.type, entity.parent);
        return {
            urn: entity.urn,
            type: EntityType.SchemaField,
            name: entity?.fieldPath,
            expandedName: `${parent?.name}.${entity?.fieldPath}`,
            icon: parent?.platform?.properties?.logoUrl ?? undefined,
            parents: parent ? [parent] : undefined,
        };
    };

    supportedCapabilities = () => new Set([]);
}
