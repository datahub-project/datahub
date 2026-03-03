import {
    DashboardOutlined,
    EyeOutlined,
    FileOutlined,
    LayoutOutlined,
    PartitionOutlined,
    UnorderedListOutlined,
    WarningOutlined,
} from '@ant-design/icons';
import { ChartLine, ListBullets, TreeStructure } from '@phosphor-icons/react';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { ChartPreview } from '@app/entityV2/chart/preview/ChartPreview';
import { ChartStatsSummarySubHeader } from '@app/entityV2/chart/profile/stats/ChartStatsSummarySubHeader';
import ChartSummaryTab from '@app/entityV2/chart/summary/ChartSummaryTab';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { SubType, TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarApplicationSection } from '@app/entityV2/shared/containers/profile/sidebar/Applications/SidebarApplicationSection';
import SidebarChartHeaderSection from '@app/entityV2/shared/containers/profile/sidebar/Chart/Header/SidebarChartHeaderSection';
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
import { ChartDashboardsTab } from '@app/entityV2/shared/tabs/Entity/ChartDashboardsTab';
import { InputFieldsTab } from '@app/entityV2/shared/tabs/Entity/InputFieldsTab';
import { IncidentTab } from '@app/entityV2/shared/tabs/Incident/IncidentTab';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import {
    SidebarTitleActionType,
    getDashboardLastUpdatedMs,
    getFirstSubType,
    isOutputPort,
} from '@app/entityV2/shared/utils';
import { LOOKER_URN, MODE, MODE_URN } from '@app/ingest/source/builder/constants';
import { MatchedFieldList } from '@app/searchV2/matches/MatchedFieldList';
import { matchedInputFieldRenderer } from '@app/searchV2/matches/matchedInputFieldRenderer';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { GetChartQuery, useGetChartQuery, useUpdateChartMutation } from '@graphql/chart.generated';
import { Chart, EntityType, LineageDirection, SearchResult } from '@types';

const PREVIEW_SUPPORTED_PLATFORMS = [LOOKER_URN, MODE_URN];

const headerDropdownItems = new Set([
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.ANNOUNCE,
]);

/**
 * Definition of the DataHub Chart entity.
 */
export class ChartEntity implements Entity<Chart> {
    type: EntityType = EntityType.Chart;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <ChartLine
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

    getGraphName = () => 'chart';

    getPathName = () => this.getGraphName();

    getEntityName = () => 'Chart';

    getCollectionName = () => 'Charts';

    useEntityQuery = useGetChartQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Chart}
            useEntityQuery={useGetChartQuery}
            useUpdateQuery={useUpdateChartMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            subHeader={{
                component: ChartStatsSummarySubHeader,
            }}
            tabs={[
                {
                    name: 'Summary',
                    component: ChartSummaryTab,
                    icon: SUMMARY_TAB_ICON,
                    display: {
                        visible: (_, chart: GetChartQuery) =>
                            !!chart?.chart?.subTypes?.typeNames?.includes(SubType.TableauWorksheet) ||
                            !!chart?.chart?.subTypes?.typeNames?.includes(SubType.Looker) ||
                            chart?.chart?.platform?.name === MODE,
                        enabled: () => true,
                    },
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                    icon: FileOutlined,
                },
                {
                    name: 'Fields',
                    component: InputFieldsTab,
                    icon: LayoutOutlined,
                    display: {
                        visible: (_, chart: GetChartQuery) => (chart?.chart?.inputFields?.fields?.length || 0) > 0,
                        enabled: (_, chart: GetChartQuery) => (chart?.chart?.inputFields?.fields?.length || 0) > 0,
                    },
                },
                {
                    name: 'Preview',
                    component: EmbedTab,
                    icon: EyeOutlined,
                    display: {
                        visible: (_, chart: GetChartQuery) =>
                            !!chart?.chart?.embed?.renderUrl &&
                            PREVIEW_SUPPORTED_PLATFORMS.includes(chart?.chart?.platform.urn),
                        enabled: (_, chart: GetChartQuery) =>
                            !!chart?.chart?.embed?.renderUrl &&
                            PREVIEW_SUPPORTED_PLATFORMS.includes(chart?.chart?.platform.urn),
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
                    name: 'Dashboards',
                    component: ChartDashboardsTab,
                    icon: DashboardOutlined,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, chart: GetChartQuery) => (chart?.chart?.dashboards?.total || 0) > 0,
                    },
                },
                {
                    name: 'Incidents',
                    getCount: (_, chart, loading) => {
                        return !loading ? chart?.chart?.activeIncidents?.total : undefined;
                    },
                    icon: WarningOutlined,
                    component: IncidentTab,
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
            component: SidebarChartHeaderSection,
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
            component: SidebarTagsSection,
        },
        {
            component: SidebarGlossaryTermsSection,
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

    getOverridePropertiesFromEntity = (chart?: Chart | null): GenericEntityProperties => {
        // TODO: Get rid of this once we have correctly formed platform coming back.
        const name = chart?.properties?.name;
        const subTypes = chart?.subTypes;
        const externalUrl = chart?.properties?.externalUrl;
        return {
            name,
            externalUrl,
            entityTypeOverride: subTypes ? capitalizeFirstLetterOnly(subTypes.typeNames?.[0]) : '',
        };
    };

    renderPreview = (previewType: PreviewType, data: Chart) => {
        const genericProperties = this.getGenericEntityProperties(data);

        return (
            <ChartPreview
                urn={data.urn}
                data={genericProperties}
                platform={data?.platform?.properties?.displayName || capitalizeFirstLetterOnly(data?.platform?.name)}
                name={data.properties?.name}
                description={data.editableProperties?.description || data.properties?.description}
                access={data.properties?.access}
                owners={data.ownership?.owners}
                tags={data?.globalTags || undefined}
                glossaryTerms={data?.glossaryTerms}
                logoUrl={data?.platform?.properties?.logoUrl}
                parentContainers={data.parentContainers}
                subType={getFirstSubType(data)}
                headerDropdownItems={headerDropdownItems}
                browsePaths={data.browsePathV2 || undefined}
                externalUrl={data.properties?.externalUrl}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Chart;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <ChartPreview
                urn={data.urn}
                data={genericProperties}
                subType={getFirstSubType(data)}
                platform={data?.platform?.properties?.displayName || capitalizeFirstLetterOnly(data?.platform?.name)}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                name={data.properties?.name}
                description={data.editableProperties?.description || data.properties?.description}
                access={data.properties?.access}
                owners={data.ownership?.owners}
                tags={data?.globalTags || undefined}
                glossaryTerms={data?.glossaryTerms}
                insights={result.insights}
                logoUrl={data?.platform?.properties?.logoUrl || ''}
                deprecation={data.deprecation}
                statsSummary={data.statsSummary}
                lastUpdatedMs={getDashboardLastUpdatedMs(data?.properties)}
                createdMs={data.properties?.created?.time}
                externalUrl={data.properties?.externalUrl}
                snippet={
                    <MatchedFieldList
                        customFieldRenderer={(matchedField) => matchedInputFieldRenderer(matchedField, data)}
                    />
                }
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
        const data = result.entity as Chart;
        return (
            <MatchedFieldList customFieldRenderer={(matchedField) => matchedInputFieldRenderer(matchedField, data)} />
        );
    };

    getLineageVizConfig = (entity: Chart) => {
        return {
            urn: entity.urn,
            name: entity.properties?.name || entity.urn,
            type: EntityType.Chart,
            icon: entity?.platform?.properties?.logoUrl || undefined,
            platform: entity?.platform,
            subtype: getFirstSubType(entity) || undefined,
            deprecation: entity?.deprecation,
        };
    };

    displayName = (data: Chart) => {
        return data.properties?.name || data.urn;
    };

    getGenericEntityProperties = (data: Chart) => {
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

    renderEmbeddedProfile = (urn: string) => (
        <EmbeddedProfile
            urn={urn}
            entityType={EntityType.Chart}
            useEntityQuery={useGetChartQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
        />
    );
}
