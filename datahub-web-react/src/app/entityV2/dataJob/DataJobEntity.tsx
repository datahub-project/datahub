import { ConsoleSqlOutlined } from '@ant-design/icons';
import { ArrowsClockwise, FileText, ListBullets, Share, TreeStructure, WarningCircle } from '@phosphor-icons/react';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { DataFlowEntity } from '@app/entityV2/dataFlow/DataFlowEntity';
import { Preview } from '@app/entityV2/dataJob/preview/Preview';
import { RunsTab } from '@app/entityV2/dataJob/tabs/RunsTab';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarApplicationSection } from '@app/entityV2/shared/containers/profile/sidebar/Applications/SidebarApplicationSection';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import SidebarLineageSection from '@app/entityV2/shared/containers/profile/sidebar/Lineage/SidebarLineageSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarQueryOperationsSection from '@app/entityV2/shared/containers/profile/sidebar/Query/SidebarQueryOperationsSection';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarDataJobTransformationLogicSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarLogicSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import SidebarNotesSection from '@app/entityV2/shared/sidebarSection/SidebarNotesSection';
import SidebarStructuredProperties from '@app/entityV2/shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { DataJobFlowTab } from '@app/entityV2/shared/tabs/Entity/DataJobFlowTab';
import TabNameWithCount from '@app/entityV2/shared/tabs/Entity/TabNameWithCount';
import { IncidentTab } from '@app/entityV2/shared/tabs/Incident/IncidentTab';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { SidebarTitleActionType, getDataProduct, isOutputPort } from '@app/entityV2/shared/utils';
import { EntityAndType } from '@app/lineage/types';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { GetDataJobQuery, useGetDataJobQuery, useUpdateDataJobMutation } from '@graphql/dataJob.generated';
import { DataJob, EntityType, SearchResult } from '@types';

const getDataJobPlatformName = (data?: DataJob): string => {
    return (
        data?.dataFlow?.platform?.properties?.displayName ||
        capitalizeFirstLetterOnly(data?.dataFlow?.platform?.name) ||
        ''
    );
};

const headerDropdownItems = new Set([
    EntityMenuItems.EXTERNAL_URL,
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.ANNOUNCE,
]);

/**
 * Definition of the DataHub DataJob entity.
 */
export class DataJobEntity implements Entity<DataJob> {
    type: EntityType = EntityType.DataJob;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <ConsoleSqlOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <ConsoleSqlOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#B37FEB' }} />
            );
        }

        return (
            <ConsoleSqlOutlined
                className={TYPE_ICON_CLASS_NAME}
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
            useEntityQuery={useGetDataJobQuery}
            useUpdateQuery={useUpdateDataJobMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            tabs={[
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                    icon: FileText,
                },
                {
                    name: 'Pipeline',
                    component: DataJobFlowTab,
                    icon: Share,
                },
                {
                    name: 'Lineage',
                    component: LineageTab,
                    icon: TreeStructure,
                    supportsFullsize: true,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                    icon: ListBullets,
                },
                {
                    name: 'Runs',
                    component: RunsTab,
                    icon: ArrowsClockwise,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, dataJob: GetDataJobQuery) => (dataJob?.dataJob?.runs?.total || 0) !== 0,
                    },
                },
                {
                    name: 'Incidents',
                    icon: WarningCircle,
                    component: IncidentTab,
                    getDynamicName: (_, dataJob, loading) => {
                        const activeIncidentCount = dataJob?.dataJob?.activeIncidents?.total;
                        return <TabNameWithCount name="Incidents" count={activeIncidentCount} loading={loading} />;
                    },
                },
            ]}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    getSidebarSections = () => [
        { component: SidebarEntityHeader },
        { component: SidebarQueryOperationsSection },
        { component: SidebarAboutSection },
        { component: SidebarNotesSection },
        { component: SidebarLineageSection },
        { component: SidebarDataJobTransformationLogicSection },
        { component: SidebarOwnerSection },
        { component: SidebarDomainSection },
        { component: SidebarApplicationSection },
        { component: DataProductSection },
        { component: SidebarGlossaryTermsSection },
        { component: SidebarTagsSection },
        {
            component: SidebarStructuredProperties,
        },
        { component: StatusSection },
    ];

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
        {
            name: 'Properties',
            component: PropertiesTab,
            description: 'View additional properties about this asset',
            icon: ListBullets,
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

    renderPreview = (previewType: PreviewType, data: DataJob) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.properties?.name || ''}
                subtype={data.subTypes?.typeNames?.[0]}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={getDataJobPlatformName(data)}
                platformLogo={data?.dataFlow?.platform?.properties?.logoUrl || ''}
                owners={data.ownership?.owners}
                globalTags={data.globalTags || null}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                externalUrl={data.properties?.externalUrl}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
                browsePaths={data?.browsePathV2 || undefined}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataJob;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.properties?.name || ''}
                subtype={data.subTypes?.typeNames?.[0]}
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
                isOutputPort={isOutputPort(result)}
                headerDropdownItems={headerDropdownItems}
                browsePaths={data?.browsePathV2 || undefined}
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
            icon: entity?.dataFlow?.platform?.properties?.logoUrl || undefined, // eslint-disable-next-line @typescript-eslint/dot-notation
            downstreamChildren: entity?.['downstream']?.relationships?.map(
                (relationship) =>
                    ({
                        entity: relationship.entity,
                        type: relationship.entity.type,
                    }) as EntityAndType,
            ), // eslint-disable-next-line @typescript-eslint/dot-notation
            upstreamChildren: entity?.['upstream']?.relationships?.map(
                (relationship) =>
                    ({
                        entity: relationship.entity,
                        type: relationship.entity.type,
                    }) as EntityAndType,
            ),
            platform: entity?.dataFlow?.platform,
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
            EntityCapabilityType.TEST,
            EntityCapabilityType.LINEAGE,
            EntityCapabilityType.HEALTH,
            EntityCapabilityType.APPLICATIONS,
        ]);
    };
}
