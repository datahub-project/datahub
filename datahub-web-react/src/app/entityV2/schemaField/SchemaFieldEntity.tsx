import * as React from 'react';
import TabFullsizedContext from '@src/app/shared/TabFullsizedContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { globalEntityRegistryV2 } from '@app/EntityRegistryProvider';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { FetchedEntity } from '@app/lineage/types';
import { useGetSchemaFieldQuery } from '@graphql/schemaField.generated';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { downgradeV2FieldPath } from '@app/lineageV2/lineageUtils';
import { decodeSchemaField } from '@app/lineage/utils/columnLineageUtils';
import { PartitionOutlined, PicCenterOutlined, UnorderedListOutlined } from '@ant-design/icons';
import { EntityType, SchemaFieldEntity as SchemaField, SearchResult } from '@types';

import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { Preview } from './preview/Preview';
import SidebarNotesSection from '../shared/sidebarSection/SidebarNotesSection';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';

const headerDropdownItems = new Set([EntityMenuItems.SHARE, EntityMenuItems.ANNOUNCE]);

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

    getEntityName = () => 'Column';

    getCollectionName = () => 'Columns';

    useEntityQuery = useGetSchemaFieldQuery;

    renderProfile = (urn: string) => (
        <TabFullsizedContext.Provider value={{ isTabFullsize: true }}>
            <EntityProfile
                urn={urn}
                entityType={EntityType.SchemaField}
                useEntityQuery={useGetSchemaFieldQuery}
                headerDropdownItems={headerDropdownItems}
                tabs={[
                    {
                        name: 'Lineage',
                        component: LineageTab,
                        icon: PartitionOutlined,
                    },
                    {
                        name: 'Properties',
                        component: PropertiesTab,
                        icon: UnorderedListOutlined,
                    },
                ]}
                sidebarSections={this.getSidebarSections()}
            />
        </TabFullsizedContext.Provider>
    );

    getSidebarSections = () => [{ component: SidebarEntityHeader }, { component: SidebarNotesSection }];

    getGraphName = () => 'schemaField';

    renderPreview = (previewType: PreviewType, data: SchemaField) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                data={genericProperties}
                previewType={previewType}
                datasetUrn={data.parent.urn}
                name={downgradeV2FieldPath(data.fieldPath) || data.urn}
                parent={data?.parent as GenericEntityProperties}
            />
        );
    };

    renderSearch = (result: SearchResult) => this.renderPreview(PreviewType.SEARCH, result.entity as SchemaField);

    displayName = (data: SchemaField) => decodeSchemaField(downgradeV2FieldPath(data?.fieldPath) || '') || data.urn;

    getGenericEntityProperties = (data: SchemaField): GenericEntityProperties | null =>
        getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: (newData) => newData,
        });

    getLineageVizConfig = (entity: SchemaField): FetchedEntity => {
        const parent =
            entity.parent && globalEntityRegistryV2.getGenericEntityProperties(entity.parent.type, entity.parent);
        return {
            urn: entity.urn,
            type: EntityType.SchemaField,
            name: entity?.fieldPath,
            expandedName: `${parent?.name}.${entity?.fieldPath}`,
            icon: parent?.platform?.properties?.logoUrl ?? undefined,
            parent: parent ?? undefined,
        };
    };

    supportedCapabilities = () => new Set([]);
}
