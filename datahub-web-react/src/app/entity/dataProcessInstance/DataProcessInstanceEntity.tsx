import React from 'react';
import { ApiOutlined } from '@ant-design/icons';
import { Entity as GraphQLEntity } from '@types';
import { DataProcessInstance, EntityType, OwnershipType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import {
    GetDataProcessInstanceQuery,
    useGetDataProcessInstanceQuery,
} from '../../../graphql/dataProcessInstance.generated';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { LineageTab } from '../shared/tabs/Lineage/LineageTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { GenericEntityProperties } from '../shared/types';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityDropdown';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { getDataProduct } from '../shared/utils';
import SummaryTab from './profile/DataProcessInstanceSummary';

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

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <ApiOutlined style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <ApiOutlined style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        return (
            <ApiOutlined
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
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={new Set([EntityMenuItems.UPDATE_DEPRECATION, EntityMenuItems.RAISE_INCIDENT])}
            tabs={[
                {
                    name: 'Summary',
                    component: SummaryTab,
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

    getSidebarSections = () => [
        {
            component: SidebarAboutSection,
        },
        {
            component: SidebarOwnerSection,
            properties: {
                defaultOwnerType: OwnershipType.TechnicalOwner,
            },
        },
        {
            component: SidebarTagsSection,
            properties: {
                hasTags: true,
                hasTerms: true,
            },
        },
        {
            component: SidebarDomainSection,
        },
        {
            component: DataProductSection,
        },
    ];

    getOverridePropertiesFromEntity = (processInstance?: DataProcessInstance | null): GenericEntityProperties => {
        return {
            name: processInstance && this.displayName(processInstance),
            platform: (processInstance as GetDataProcessInstanceQuery['dataProcessInstance'])?.optionalPlatform,
        };
    };

    renderPreview = (_: PreviewType, data: DataProcessInstance) => {
        const genericProperties = this.getGenericEntityProperties(data);
        const parentEntities = getParentEntities(data);
        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                subType={data.subTypes?.typeNames?.[0]}
                description=""
                platformName={genericProperties?.platform?.properties?.displayName ?? undefined}
                platformLogo={genericProperties?.platform?.properties?.logoUrl}
                owners={null}
                globalTags={null}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                externalUrl={data.properties?.externalUrl}
                parentContainers={data.parentContainers}
                parentEntities={parentEntities}
                container={data.container || undefined}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataProcessInstance;
        const genericProperties = this.getGenericEntityProperties(data);
        const parentEntities = getParentEntities(data);

        const firstState = data?.state && data.state.length > 0 ? data.state[0] : undefined;

        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                subType={data.subTypes?.typeNames?.[0]}
                description=""
                platformName={genericProperties?.platform?.properties?.displayName ?? undefined}
                platformLogo={genericProperties?.platform?.properties?.logoUrl}
                platformInstanceId={genericProperties?.dataPlatformInstance?.instanceId}
                owners={null}
                globalTags={null}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                insights={result.insights}
                externalUrl={data.properties?.externalUrl}
                degree={(result as any).degree}
                paths={(result as any).paths}
                parentContainers={data.parentContainers}
                parentEntities={parentEntities}
                container={data.container || undefined}
                dataProcessInstanceProps={{
                    startTime: firstState?.timestampMillis,
                    duration: firstState?.durationMillis ?? undefined,
                    status: firstState?.result?.resultType ?? undefined,
                }}
            />
        );
    };

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
