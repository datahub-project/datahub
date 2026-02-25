import { PartitionOutlined, UnorderedListOutlined } from '@ant-design/icons';
import { Rows, TreeStructure } from '@phosphor-icons/react';
import * as React from 'react';

import { globalEntityRegistryV2 } from '@app/EntityRegistryProvider';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/schemaField/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import SidebarNotesSection from '@app/entityV2/shared/sidebarSection/SidebarNotesSection';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { SidebarTitleActionType } from '@app/entityV2/shared/utils';
import { FetchedEntity } from '@app/lineage/types';
import { decodeSchemaField } from '@app/lineage/utils/columnLineageUtils';
import { downgradeV2FieldPath } from '@app/lineageV2/lineageUtils';
import TabFullsizedContext from '@src/app/shared/TabFullsizedContext';

import { useGetSchemaFieldQuery } from '@graphql/schemaField.generated';
import { EntityType, SchemaFieldEntity as SchemaField, SearchResult } from '@types';

const headerDropdownItems = new Set([EntityMenuItems.SHARE, EntityMenuItems.ANNOUNCE]);

export class SchemaFieldEntity implements Entity<SchemaField> {
    type: EntityType = EntityType.SchemaField;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => (
        <Rows
            className={TYPE_ICON_CLASS_NAME}
            size={fontSize || 14}
            color={color || 'currentColor'}
            weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
        />
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
                        supportsFullsize: true,
                    },
                    {
                        name: 'Properties',
                        component: PropertiesTab,
                        icon: UnorderedListOutlined,
                    },
                ]}
                sidebarSections={this.getSidebarSections()}
                sidebarTabs={this.getSidebarTabs()}
            />
        </TabFullsizedContext.Provider>
    );

    getSidebarSections = () => [{ component: SidebarEntityHeader }, { component: SidebarNotesSection }];

    getSidebarTabs = () => [
        {
            name: 'Lineage',
            component: LineageTab,
            description: "View this data asset's upstream and downstream dependencies",
            icon: TreeStructure,
            properties: {
                actionType: SidebarTitleActionType.LineageExplore,
            },
        },
    ];

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

    getOverridePropertiesFromEntity = (schemaField?: SchemaField | null): GenericEntityProperties => {
        const parent =
            schemaField?.parent &&
            globalEntityRegistryV2.getGenericEntityProperties(schemaField?.parent.type, schemaField?.parent);
        return {
            platform: parent?.platform,
        };
    };

    getGenericEntityProperties = (data: SchemaField): GenericEntityProperties | null =>
        getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
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
