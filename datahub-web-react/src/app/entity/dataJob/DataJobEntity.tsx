import { ConsoleSqlOutlined } from '@ant-design/icons';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entity/Entity';
import { DataFlowEntity } from '@app/entity/dataFlow/DataFlowEntity';
import { Preview } from '@app/entity/dataJob/preview/Preview';
import { RunsTab } from '@app/entity/dataJob/tabs/RunsTab';
import { EntityMenuItems } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { EntityProfile } from '@app/entity/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entity/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import DataProductSection from '@app/entity/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entity/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entity/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarTagsSection } from '@app/entity/shared/containers/profile/sidebar/SidebarTagsSection';
import SidebarStructuredPropsSection from '@app/entity/shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';
import { getDataForEntityType } from '@app/entity/shared/containers/profile/utils';
import { DocumentationTab } from '@app/entity/shared/tabs/Documentation/DocumentationTab';
import { DataJobFlowTab } from '@app/entity/shared/tabs/Entity/DataJobFlowTab';
import { IncidentTab } from '@app/entity/shared/tabs/Incident/IncidentTab';
import { LineageTab } from '@app/entity/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entity/shared/tabs/Properties/PropertiesTab';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { getDataProduct } from '@app/entity/shared/utils';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { GetDataJobQuery, useGetDataJobQuery, useUpdateDataJobMutation } from '@graphql/dataJob.generated';
import { DataJob, EntityType, OwnershipType, SearchResult } from '@types';

const getDataJobPlatformName = (data?: DataJob): string => {
    return (
        data?.dataFlow?.platform?.properties?.displayName ||
        capitalizeFirstLetterOnly(data?.dataFlow?.platform?.name) ||
        ''
    );
};

/**
 * Definition of the DataHub DataJob entity.
 */
export class DataJobEntity implements Entity<DataJob> {
    type: EntityType = EntityType.DataJob;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <ConsoleSqlOutlined style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <ConsoleSqlOutlined style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        return (
            <ConsoleSqlOutlined
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'dataJob';

    getPathName = () => 'tasks';

    getEntityName = () => 'Task';

    getCollectionName = () => 'Tasks';

    useEntityQuery = useGetDataJobQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.DataJob}
            useEntityQuery={this.useEntityQuery}
            useUpdateQuery={useUpdateDataJobMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={new Set([EntityMenuItems.UPDATE_DEPRECATION, EntityMenuItems.RAISE_INCIDENT])}
            tabs={[
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
                {
                    name: 'Pipeline',
                    component: DataJobFlowTab,
                },
                {
                    name: 'Lineage',
                    component: LineageTab,
                },
                {
                    name: 'Runs',
                    component: RunsTab,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, dataJob: GetDataJobQuery) => (dataJob?.dataJob?.runs?.total || 0) !== 0,
                    },
                },
                {
                    name: 'Incidents',
                    component: IncidentTab,
                    getDynamicName: (_, dataJob) => {
                        const activeIncidentCount = dataJob?.dataJob?.activeIncidents?.total;
                        return `Incidents${(activeIncidentCount && ` (${activeIncidentCount})`) || ''}`;
                    },
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
        {
            component: SidebarStructuredPropsSection,
        },
    ];

    getOverridePropertiesFromEntity = (dataJob?: DataJob | null): GenericEntityProperties => {
        // TODO: Get rid of this once we have correctly formed platform coming back.
        const name = dataJob?.properties?.name;
        const externalUrl = dataJob?.properties?.externalUrl;
        return {
            name,
            externalUrl,
            platform: dataJob?.dataFlow?.platform,
        };
    };

    renderPreview = (_: PreviewType, data: DataJob) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || ''}
                subType={data.subTypes?.typeNames?.[0]}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={getDataJobPlatformName(data)}
                platformLogo={data?.dataFlow?.platform?.properties?.logoUrl || ''}
                owners={data.ownership?.owners}
                globalTags={data.globalTags || null}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                externalUrl={data.properties?.externalUrl}
                health={data.health}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataJob;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || ''}
                subType={data.subTypes?.typeNames?.[0]}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={getDataJobPlatformName(data)}
                platformLogo={data?.dataFlow?.platform?.properties?.logoUrl || ''}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                deprecation={data.deprecation}
                insights={result.insights}
                externalUrl={data.properties?.externalUrl}
                lastRunTimeMs={
                    ((data as any).lastRun?.runs?.length && (data as any).lastRun?.runs[0]?.created?.time) || undefined
                }
                degree={(result as any).degree}
                paths={(result as any).paths}
                health={data.health}
                parentContainers={data.parentContainers}
            />
        );
    };

    getExpandedNameForDataJob = (entity: DataJob): string => {
        const name = this.displayName(entity);
        const flowName = entity?.dataFlow ? new DataFlowEntity().displayName(entity?.dataFlow) : undefined;

        // if we have no name, just return blank. this should not happen, so dont try & construct a name
        if (!name) {
            return '';
        }

        // if we have a flow name, return the full name of flow.task
        if (flowName) {
            return `${flowName}.${name}`;
        }

        // otherwise, just return the task name (same as non-expanded)
        return name;
    };

    getLineageVizConfig = (entity: DataJob) => {
        return {
            urn: entity?.urn,
            name: this.displayName(entity),
            expandedName: this.getExpandedNameForDataJob(entity),
            type: EntityType.DataJob,
            icon: entity?.dataFlow?.platform?.properties?.logoUrl || undefined,
            platform: entity?.dataFlow?.platform,
            health: entity?.health || undefined,
        };
    };

    displayName = (data: DataJob) => {
        return data.properties?.name || data.urn;
    };

    getGenericEntityProperties = (data: DataJob) => {
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
