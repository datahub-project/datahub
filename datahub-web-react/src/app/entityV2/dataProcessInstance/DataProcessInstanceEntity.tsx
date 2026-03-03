import { ArrowsClockwise, TreeStructure } from '@phosphor-icons/react';
import React from 'react';

import { globalEntityRegistryV2 } from '@app/EntityRegistryProvider';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import Preview from '@app/entityV2/dataProcessInstance/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { SidebarTitleActionType, getDataProduct, getFirstSubType } from '@app/entityV2/shared/utils';
import DataProcessInstanceSummary from '@src/app/entity/dataProcessInstance/profile/DataProcessInstanceSummary';

import { GetDataProcessInstanceQuery, useGetDataProcessInstanceQuery } from '@graphql/dataProcessInstance.generated';
import { DataProcessInstance, EntityType, Entity as GraphQLEntity, SearchResult } from '@types';

const getParentEntities = (data: DataProcessInstance): GraphQLEntity[] => {
    const parentEntity = data?.relationships?.relationships?.find(
        (rel) => rel.type === 'InstanceOf' && rel.entity?.type === EntityType.DataJob,
    );

    if (!parentEntity || !parentEntity.entity) {
        return [];
    }

    // First cast to unknown, then to Entity with proper type
    return [parentEntity.entity];
};

/**
 * Definition of the DataHub DataProcessInstance entity.
 */
export class DataProcessInstanceEntity implements Entity<DataProcessInstance> {
    type: EntityType = EntityType.DataProcessInstance;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <ArrowsClockwise
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
            />
        );
    };

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'dataProcessInstance';

    getEntityName = () => 'Process Instance';

    getGraphName = () => 'dataProcessInstance';

    getCollectionName = () => 'Process Instances';

    useEntityQuery = useGetDataProcessInstanceQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.DataProcessInstance}
            useEntityQuery={this.useEntityQuery}
            // useUpdateQuery={useUpdateDataProcessInstanceMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={
                new Set([EntityMenuItems.UPDATE_DEPRECATION, EntityMenuItems.RAISE_INCIDENT, EntityMenuItems.SHARE])
            }
            tabs={[
                {
                    name: 'Summary',
                    component: DataProcessInstanceSummary,
                },
                {
                    name: 'Lineage',
                    component: LineageTab,
                    supportsFullsize: true,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
            ]}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    getSidebarSections = () => [{ component: SidebarEntityHeader }];

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

    getOverridePropertiesFromEntity = (processInstance?: DataProcessInstance | null): GenericEntityProperties => {
        const parent =
            processInstance?.parentTemplate &&
            globalEntityRegistryV2.getGenericEntityProperties(
                processInstance.parentTemplate.type,
                processInstance.parentTemplate,
            );
        return {
            name: processInstance && this.displayName(processInstance),
            platform:
                (processInstance as GetDataProcessInstanceQuery['dataProcessInstance'])?.optionalPlatform ||
                parent?.platform,
            parent,
            // Not currently rendered in V2
            lastRunEvent: processInstance?.state?.[0],
        };
    };

    renderPreview = (previewType: PreviewType, data: DataProcessInstance) => {
        const genericProperties = this.getGenericEntityProperties(data);
        const parentEntities = getParentEntities(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                subType={getFirstSubType(data)}
                description=""
                platformName={genericProperties?.platform?.properties?.displayName ?? undefined}
                platformLogo={genericProperties?.platform?.properties?.logoUrl}
                owners={null}
                globalTags={null}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                externalUrl={data.properties?.externalUrl}
                parentEntities={parentEntities}
                container={data.container || undefined}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) =>
        this.renderPreview(PreviewType.SEARCH, result.entity as DataProcessInstance);

    getLineageVizConfig = (entity: DataProcessInstance) => {
        const properties = this.getGenericEntityProperties(entity);
        return {
            urn: entity?.urn,
            name: this.displayName(entity),
            type: EntityType.DataProcessInstance,
            subtype: getFirstSubType(entity),
            icon: properties?.platform?.properties?.logoUrl ?? undefined,
            platform: properties?.platform ?? undefined,
            container: entity?.container,
        };
    };

    displayName = (data: DataProcessInstance) => {
        return data.properties?.name || data.urn;
    };

    getGenericEntityProperties = (data: DataProcessInstance) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    supportedCapabilities = () => {
        return new Set([
            EntityCapabilityType.OWNERS,
            EntityCapabilityType.GLOSSARY_TERMS,
            EntityCapabilityType.TAGS,
            EntityCapabilityType.DOMAINS,
            EntityCapabilityType.DEPRECATION,
            EntityCapabilityType.SOFT_DELETE,
            EntityCapabilityType.DATA_PRODUCTS,
        ]);
    };
}
