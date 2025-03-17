import DataProcessInstanceSummary from '@src/app/entity/dataProcessInstance/profile/DataProcessInstanceSummary';
import { globalEntityRegistryV2 } from '@app/EntityRegistryProvider';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity as GraphQLEntity } from '@types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { getDataProduct } from '@app/entityV2/shared/utils';
import { GetDataProcessInstanceQuery, useGetDataProcessInstanceQuery } from '@graphql/dataProcessInstance.generated';
import { ArrowsClockwise } from 'phosphor-react';
import React from 'react';
import { DataProcessInstance, EntityType, SearchResult } from '../../../types.generated';
import Preview from './preview/Preview';

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
        if (styleType === IconStyleType.TAB_VIEW) {
            return <ArrowsClockwise style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <ArrowsClockwise style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        return (
            <ArrowsClockwise
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
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
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
            ]}
            sidebarSections={this.getSidebarSections()}
        />
    );

    getSidebarSections = () => [{ component: SidebarEntityHeader }];

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

    renderPreview = (_: PreviewType, data: DataProcessInstance) => {
        const genericProperties = this.getGenericEntityProperties(data);
        const parentEntities = getParentEntities(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                subType={data.subTypes?.typeNames?.[0]}
                description=""
                platformName={genericProperties?.platform?.properties?.displayName ?? undefined}
                platformLogo={genericProperties?.platform?.properties?.logoUrl}
                owners={null}
                globalTags={null}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                externalUrl={data.properties?.externalUrl}
                parentEntities={parentEntities}
                container={data.container || undefined}
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
            subtype: entity?.subTypes?.typeNames?.[0],
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
