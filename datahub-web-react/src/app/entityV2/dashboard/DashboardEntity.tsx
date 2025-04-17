import {
    AppstoreOutlined,
    DashboardFilled,
    DashboardOutlined,
    EyeOutlined,
    FileOutlined,
    PartitionOutlined,
    TableOutlined,
    UnorderedListOutlined,
    WarningOutlined,
} from '@ant-design/icons';
import { ListBullets, TreeStructure } from '@phosphor-icons/react';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { DashboardPreview } from '@app/entityV2/dashboard/preview/DashboardPreview';
import { DashboardStatsSummarySubHeader } from '@app/entityV2/dashboard/profile/DashboardStatsSummarySubHeader';
import DashboardSummaryTab from '@app/entityV2/dashboard/summary/DashboardSummaryTab';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import SidebarDashboardHeaderSection from '@app/entityV2/shared/containers/profile/sidebar/Dashboard/Header/SidebarDashboardHeaderSection';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import SidebarLineageSection from '@app/entityV2/shared/containers/profile/sidebar/Lineage/SidebarLineageSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import EmbeddedProfile from '@app/entityV2/shared/embed/EmbeddedProfile';
import SidebarNotesSection from '@app/entityV2/shared/sidebarSection/SidebarNotesSection';
import SidebarStructuredProperties from '@app/entityV2/shared/sidebarSection/SidebarStructuredProperties';
import { SUMMARY_TAB_ICON } from '@app/entityV2/shared/summary/HeaderComponents';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { EmbedTab } from '@app/entityV2/shared/tabs/Embed/EmbedTab';
import { DashboardChartsTab } from '@app/entityV2/shared/tabs/Entity/DashboardChartsTab';
import { DashboardDatasetsTab } from '@app/entityV2/shared/tabs/Entity/DashboardDatasetsTab';
import TabNameWithCount from '@app/entityV2/shared/tabs/Entity/TabNameWithCount';
import { IncidentTab } from '@app/entityV2/shared/tabs/Incident/IncidentTab';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import {
    SidebarTitleActionType,
    getDashboardLastUpdatedMs,
    getDataProduct,
    isOutputPort,
} from '@app/entityV2/shared/utils';
import { LOOKER_URN, MODE_URN } from '@app/ingest/source/builder/constants';
import { matchedInputFieldRenderer } from '@app/search/matches/matchedInputFieldRenderer';
import { MatchedFieldList } from '@app/searchV2/matches/MatchedFieldList';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { GetDashboardQuery, useGetDashboardQuery, useUpdateDashboardMutation } from '@graphql/dashboard.generated';
import { Dashboard, EntityType, LineageDirection, SearchResult } from '@types';

const PREVIEW_SUPPORTED_PLATFORMS = [LOOKER_URN, MODE_URN];

/**
 * Definition of the DataHub Dashboard entity.
 */

const headerDropdownItems = new Set([
    EntityMenuItems.EXTERNAL_URL,
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.ANNOUNCE,
]);

export class DashboardEntity implements Entity<Dashboard> {
    type: EntityType = EntityType.Dashboard;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DashboardOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <DashboardFilled
                    className={TYPE_ICON_CLASS_NAME}
                    style={{ fontSize, color: color || 'rgb(144 163 236)' }}
                />
            );
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M924.8 385.6a446.7 446.7 0 00-96-142.4 446.7 446.7 0 00-142.4-96C631.1 123.8 572.5 112 512 112s-119.1 11.8-174.4 35.2a446.7 446.7 0 00-142.4 96 446.7 446.7 0 00-96 142.4C75.8 440.9 64 499.5 64 560c0 132.7 58.3 257.7 159.9 343.1l1.7 1.4c5.8 4.8 13.1 7.5 20.6 7.5h531.7c7.5 0 14.8-2.7 20.6-7.5l1.7-1.4C901.7 817.7 960 692.7 960 560c0-60.5-11.9-119.1-35.2-174.4zM761.4 836H262.6A371.12 371.12 0 01140 560c0-99.4 38.7-192.8 109-263 70.3-70.3 163.7-109 263-109 99.4 0 192.8 38.7 263 109 70.3 70.3 109 163.7 109 263 0 105.6-44.5 205.5-122.6 276zM623.5 421.5a8.03 8.03 0 00-11.3 0L527.7 506c-18.7-5-39.4-.2-54.1 14.5a55.95 55.95 0 000 79.2 55.95 55.95 0 0079.2 0 55.87 55.87 0 0014.5-54.1l84.5-84.5c3.1-3.1 3.1-8.2 0-11.3l-28.3-28.3zM490 320h44c4.4 0 8-3.6 8-8v-80c0-4.4-3.6-8-8-8h-44c-4.4 0-8 3.6-8 8v80c0 4.4 3.6 8 8 8zm260 218v44c0 4.4 3.6 8 8 8h80c4.4 0 8-3.6 8-8v-44c0-4.4-3.6-8-8-8h-80c-4.4 0-8 3.6-8 8zm12.7-197.2l-31.1-31.1a8.03 8.03 0 00-11.3 0l-56.6 56.6a8.03 8.03 0 000 11.3l31.1 31.1c3.1 3.1 8.2 3.1 11.3 0l56.6-56.6c3.1-3.1 3.1-8.2 0-11.3zm-458.6-31.1a8.03 8.03 0 00-11.3 0l-31.1 31.1a8.03 8.03 0 000 11.3l56.6 56.6c3.1 3.1 8.2 3.1 11.3 0l31.1-31.1c3.1-3.1 3.1-8.2 0-11.3l-56.6-56.6zM262 530h-80c-4.4 0-8 3.6-8 8v44c0 4.4 3.6 8 8 8h80c4.4 0 8-3.6 8-8v-44c0-4.4-3.6-8-8-8z" />
            );
        }

        return (
            <DashboardOutlined
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'title';

    getPathName = () => 'dashboard';

    getEntityName = () => 'Dashboard';

    getCollectionName = () => 'Dashboards';

    useEntityQuery = useGetDashboardQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Dashboard}
            useEntityQuery={useGetDashboardQuery}
            useUpdateQuery={useUpdateDashboardMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            subHeader={{
                component: DashboardStatsSummarySubHeader,
            }}
            tabs={[
                {
                    name: 'Summary',
                    component: DashboardSummaryTab,
                    icon: SUMMARY_TAB_ICON,
                },
                {
                    name: 'Contents',
                    component: DashboardChartsTab,
                    icon: AppstoreOutlined,
                    display: {
                        visible: (_, dashboard: GetDashboardQuery) =>
                            (dashboard?.dashboard?.charts?.total || 0) > 0 ||
                            (dashboard?.dashboard?.datasets?.total || 0) === 0,
                        enabled: (_, dashboard: GetDashboardQuery) => (dashboard?.dashboard?.charts?.total || 0) > 0,
                    },
                },
                {
                    name: 'Datasets',
                    component: DashboardDatasetsTab,
                    icon: TableOutlined,
                    display: {
                        visible: (_, dashboard: GetDashboardQuery) => (dashboard?.dashboard?.datasets?.total || 0) > 0,
                        enabled: (_, dashboard: GetDashboardQuery) => (dashboard?.dashboard?.datasets?.total || 0) > 0,
                    },
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                    icon: FileOutlined,
                },
                {
                    name: 'Preview',
                    component: EmbedTab,
                    icon: EyeOutlined,
                    display: {
                        visible: (_, dashboard: GetDashboardQuery) =>
                            !!dashboard?.dashboard?.embed?.renderUrl &&
                            PREVIEW_SUPPORTED_PLATFORMS.includes(dashboard?.dashboard?.platform.urn),
                        enabled: (_, dashboard: GetDashboardQuery) =>
                            !!dashboard?.dashboard?.embed?.renderUrl &&
                            PREVIEW_SUPPORTED_PLATFORMS.includes(dashboard?.dashboard?.platform.urn),
                    },
                },
                {
                    name: 'Lineage',
                    component: LineageTab,
                    icon: PartitionOutlined,
                    properties: {
                        defaultDirection: LineageDirection.Upstream,
                    },
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                    icon: UnorderedListOutlined,
                },
                {
                    name: 'Incidents',
                    icon: WarningOutlined,
                    component: IncidentTab,
                    getDynamicName: (_, dashboard, loading) => {
                        const activeIncidentCount = dashboard?.dashboard?.activeIncidents?.total;
                        return <TabNameWithCount name="Incidents" count={activeIncidentCount} loading={loading} />;
                    },
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
            component: SidebarDashboardHeaderSection,
        },
        {
            component: SidebarAboutSection,
        },
        {
            component: SidebarNotesSection,
        },
        {
            component: SidebarLineageSection,
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
            name: 'Lineage',
            component: LineageTab,
            description: "View this data asset's upstream and downstream dependencies",
            icon: TreeStructure,
            properties: {
                defaultDirection: LineageDirection.Upstream,
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

    getOverridePropertiesFromEntity = (dashboard?: Dashboard | null): GenericEntityProperties => {
        // TODO: Get rid of this once we have correctly formed platform coming back.
        const name = dashboard?.properties?.name;
        const externalUrl = dashboard?.properties?.externalUrl;
        const subTypes = dashboard?.subTypes;
        return {
            name,
            externalUrl,
            entityTypeOverride: subTypes ? capitalizeFirstLetterOnly(subTypes.typeNames?.[0]) : '',
        };
    };

    renderPreview = (previewType: PreviewType, data: Dashboard) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <DashboardPreview
                urn={data.urn}
                data={genericProperties}
                platform={data?.platform?.properties?.displayName || capitalizeFirstLetterOnly(data?.platform?.name)}
                name={data.properties?.name}
                description={data.editableProperties?.description || data.properties?.description}
                access={data.properties?.access}
                tags={data.globalTags || undefined}
                owners={data.ownership?.owners}
                glossaryTerms={data?.glossaryTerms}
                logoUrl={data?.platform?.properties?.logoUrl}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                container={data.container}
                parentContainers={data.parentContainers}
                deprecation={data.deprecation}
                externalUrl={data.properties?.externalUrl}
                statsSummary={data.statsSummary}
                lastUpdatedMs={getDashboardLastUpdatedMs(data.properties)}
                createdMs={data.properties?.created?.time}
                subtype={data.subTypes?.typeNames?.[0]}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
                browsePaths={data.browsePathV2 || undefined}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Dashboard;
        const genericProperties = this.getGenericEntityProperties(data);

        return (
            <DashboardPreview
                urn={data.urn}
                data={genericProperties}
                platform={data?.platform?.properties?.displayName || capitalizeFirstLetterOnly(data?.platform?.name)}
                name={data.properties?.name}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                description={data.editableProperties?.description || data.properties?.description}
                access={data.properties?.access}
                tags={data.globalTags || undefined}
                owners={data.ownership?.owners}
                glossaryTerms={data?.glossaryTerms}
                insights={result.insights}
                logoUrl={data?.platform?.properties?.logoUrl || ''}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                container={data.container}
                parentContainers={data.parentContainers}
                deprecation={data.deprecation}
                externalUrl={data.properties?.externalUrl}
                statsSummary={data.statsSummary}
                lastUpdatedMs={getDashboardLastUpdatedMs(data.properties)}
                createdMs={data.properties?.created?.time}
                snippet={
                    <MatchedFieldList
                        customFieldRenderer={(matchedField) => matchedInputFieldRenderer(matchedField, data)}
                        matchSuffix="on a contained chart"
                    />
                }
                subtype={data.subTypes?.typeNames?.[0]}
                degree={(result as any).degree}
                paths={(result as any).paths}
                isOutputPort={isOutputPort(result)}
                headerDropdownItems={headerDropdownItems}
                browsePaths={data.browsePathV2 || undefined}
            />
        );
    };

    renderSearchMatches = (result: SearchResult) => {
        const data = result.entity as Dashboard;
        return (
            <MatchedFieldList
                customFieldRenderer={(matchedField) => matchedInputFieldRenderer(matchedField, data)}
                matchSuffix="on a contained chart"
            />
        );
    };

    getLineageVizConfig = (entity: Dashboard) => {
        return {
            urn: entity.urn,
            name: entity.properties?.name || entity.urn,
            type: EntityType.Dashboard,
            subtype: entity?.subTypes?.typeNames?.[0] || undefined,
            icon: entity?.platform?.properties?.logoUrl || undefined,
            platform: entity?.platform,
            deprecation: entity?.deprecation,
        };
    };

    displayName = (data: Dashboard) => {
        return data.properties?.name || data.urn;
    };

    getGenericEntityProperties = (data: Dashboard) => {
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

    getGraphName = () => this.getPathName();

    renderEmbeddedProfile = (urn: string) => (
        <EmbeddedProfile
            urn={urn}
            entityType={EntityType.Dashboard}
            useEntityQuery={useGetDashboardQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
        />
    );
}
