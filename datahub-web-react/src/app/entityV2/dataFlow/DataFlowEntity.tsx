import { ShareAltOutlined } from '@ant-design/icons';
import { FileText, ListBullets, Share, WarningCircle } from '@phosphor-icons/react';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/dataFlow/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import SidebarNotesSection from '@app/entityV2/shared/sidebarSection/SidebarNotesSection';
import SidebarStructuredProperties from '@app/entityV2/shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { DataFlowJobsTab } from '@app/entityV2/shared/tabs/Entity/DataFlowJobsTab';
import TabNameWithCount from '@app/entityV2/shared/tabs/Entity/TabNameWithCount';
import { IncidentTab } from '@app/entityV2/shared/tabs/Incident/IncidentTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { getDataProduct, isOutputPort } from '@app/entityV2/shared/utils';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { useGetDataFlowQuery, useUpdateDataFlowMutation } from '@graphql/dataFlow.generated';
import { DataFlow, EntityType, SearchResult } from '@types';

const headerDropdownItems = new Set([
    EntityMenuItems.EXTERNAL_URL,
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.ANNOUNCE,
]);

/**
 * Definition of the DataHub DataFlow entity.
 */
export class DataFlowEntity implements Entity<DataFlow> {
    type: EntityType = EntityType.DataFlow;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <ShareAltOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <ShareAltOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#d6246c' }} />
            );
        }

        return (
            <ShareAltOutlined
                className={TYPE_ICON_CLASS_NAME}
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'dataFlow';

    getPathName = () => 'pipelines';

    getEntityName = () => 'Pipeline';

    getCollectionName = () => 'Pipelines';

    useEntityQuery = useGetDataFlowQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.DataFlow}
            useEntityQuery={useGetDataFlowQuery}
            useUpdateQuery={useUpdateDataFlowMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            tabs={[
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                    icon: FileText,
                },
                {
                    name: 'Tasks',
                    component: DataFlowJobsTab,
                    icon: Share,
                    properties: {
                        urn,
                    },
                },
                {
                    name: 'Incidents',
                    icon: WarningCircle,
                    component: IncidentTab,
                    getDynamicName: (_, dataFlow, loading) => {
                        const activeIncidentCount = dataFlow?.dataFlow?.activeIncidents?.total;
                        return <TabNameWithCount name="Incidents" count={activeIncidentCount} loading={loading} />;
                    },
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                    icon: ListBullets,
                },
            ]}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    getSidebarSections = () => [
        {
            component: SidebarEntityHeader,
        },
        {
            component: SidebarAboutSection,
        },
        {
            component: SidebarNotesSection,
        },
        {
            component: SidebarOwnerSection,
        },
        {
            component: SidebarDomainSection,
        },
        {
            component: DataProductSection,
        },
        {
            component: SidebarGlossaryTermsSection,
        },
        {
            component: SidebarTagsSection,
        },
        {
            component: SidebarStructuredProperties,
        },
        {
            component: StatusSection,
        },
    ];

    getSidebarTabs = () => [
        {
            name: 'Properties',
            component: PropertiesTab,
            description: 'View additional properties about this asset',
            icon: ListBullets,
        },
    ];

    getOverridePropertiesFromEntity = (dataFlow?: DataFlow | null): GenericEntityProperties => {
        // TODO: Get rid of this once we have correctly formed platform coming back.
        const name = dataFlow?.properties?.name;
        const externalUrl = dataFlow?.properties?.externalUrl;
        return {
            name,
            externalUrl,
        };
    };

    renderPreview = (previewType: PreviewType, data: DataFlow) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.properties?.name || ''}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={
                    data?.platform?.properties?.displayName || capitalizeFirstLetterOnly(data?.platform?.name)
                }
                platformLogo={data?.platform?.properties?.logoUrl || ''}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                externalUrl={data.properties?.externalUrl}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataFlow;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.properties?.name || ''}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                description={data.editableProperties?.description || data.properties?.description || ''}
                platformName={
                    data?.platform?.properties?.displayName || capitalizeFirstLetterOnly(data?.platform?.name)
                }
                platformLogo={data?.platform?.properties?.logoUrl || ''}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
                insights={result.insights}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                externalUrl={data.properties?.externalUrl}
                jobCount={(data as any).childJobs?.total}
                deprecation={data.deprecation}
                degree={(result as any).degree}
                paths={(result as any).paths}
                isOutputPort={isOutputPort(result)}
                headerDropdownItems={headerDropdownItems}
                parentContainers={data.parentContainers}
            />
        );
    };

    displayName = (data: DataFlow) => {
        return data.properties?.name || data.urn;
    };

    getGenericEntityProperties = (data: DataFlow) => {
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
        ]);
    };
}
