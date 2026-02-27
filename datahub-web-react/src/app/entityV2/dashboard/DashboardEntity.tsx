import {
    AppstoreOutlined,
    EyeOutlined,
    FileOutlined,
    PartitionOutlined,
    TableOutlined,
    UnorderedListOutlined,
    WarningOutlined,
} from '@ant-design/icons';
import { ChartBar, ListBullets, TreeStructure } from '@phosphor-icons/react';
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
import { SidebarApplicationSection } from '@app/entityV2/shared/containers/profile/sidebar/Applications/SidebarApplicationSection';
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
import { IncidentTab } from '@app/entityV2/shared/tabs/Incident/IncidentTab';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import {
    SidebarTitleActionType,
    getDashboardLastUpdatedMs,
    getFirstSubType,
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
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.ANNOUNCE,
]);

export class DashboardEntity implements Entity<Dashboard> {
    type: EntityType = EntityType.Dashboard;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <ChartBar
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
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
                    supportsFullsize: true,
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
                    getCount: (_, dashboard) => {
                        return dashboard?.dashboard?.activeIncidents?.total;
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
            component: SidebarApplicationSection,
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
                container={data.container}
                parentContainers={data.parentContainers}
                deprecation={data.deprecation}
                externalUrl={data.properties?.externalUrl}
                statsSummary={data.statsSummary}
                lastUpdatedMs={getDashboardLastUpdatedMs(data.properties)}
                createdMs={data.properties?.created?.time}
                subtype={getFirstSubType(data)}
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
                subtype={getFirstSubType(data)}
                degree={(result as any).degree}
                paths={(result as any).paths}
                isOutputPort={isOutputPort(result)}
                headerDropdownItems={headerDropdownItems}
                browsePaths={data.browsePathV2 || undefined}
                previewType={PreviewType.SEARCH}
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
            subtype: getFirstSubType(entity) || undefined,
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
            EntityCapabilityType.APPLICATIONS,
            EntityCapabilityType.RELATED_DOCUMENTS,
            EntityCapabilityType.FORMS,
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
