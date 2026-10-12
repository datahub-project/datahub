import { ChartLine } from '@phosphor-icons/react/dist/csr/ChartLine';
import { Eye } from '@phosphor-icons/react/dist/csr/Eye';
import { File } from '@phosphor-icons/react/dist/csr/File';
import { Gauge } from '@phosphor-icons/react/dist/csr/Gauge';
import { Layout } from '@phosphor-icons/react/dist/csr/Layout';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import i18next from 'i18next';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { ChartPreview } from '@app/entityV2/chart/preview/ChartPreview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { SubType, TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/entityData';
import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import {
    DataProductSection,
    DocumentationTab,
    EmbedTab,
    EmbeddedProfile,
    EntityProfile,
    IncidentTab,
    LineageTab,
    PropertiesTab,
    SidebarAboutSection,
    SidebarApplicationSection,
    SidebarDomainSection,
    SidebarEntityHeader,
    SidebarGlossaryTermsSection,
    SidebarLineageSection,
    SidebarNotesSection,
    SidebarOwnerSection,
    SidebarStructuredProperties,
    SidebarTagsSection,
    StatusSection,
    SummaryTab,
} from '@app/entityV2/shared/profileChunks';
import { SUMMARY_TAB_ICON } from '@app/entityV2/shared/summary/HeaderComponents';
import { EntityTab } from '@app/entityV2/shared/types';
import {
    SidebarTitleActionType,
    getDashboardLastUpdatedMs,
    getFirstSubType,
    isOutputPort,
} from '@app/entityV2/shared/utils';
import { useShowAssetSummaryPage } from '@app/entityV2/summary/useShowAssetSummaryPage';
import { LOOKER_URN, MODE, MODE_URN } from '@app/ingest/source/builder/constants';
import { MatchedFieldList } from '@app/searchV2/matches/MatchedFieldList';
import { matchedInputFieldRenderer } from '@app/searchV2/matches/matchedInputFieldRenderer';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { GetChartQuery, useGetChartQuery, useUpdateChartMutation } from '@graphql/chart.generated';
import { Chart, EntityType, LineageDirection, SearchResult } from '@types';

const ChartSummaryTab = lazyProfileComponent(
    'ChartSummaryTab',
    () => import('@app/entityV2/chart/summary/ChartSummaryTab'),
);

const ChartStatsSummarySubHeader = lazyProfileComponent('ChartStatsSummarySubHeader', () =>
    import('@app/entityV2/chart/profile/stats/ChartStatsSummarySubHeader').then((module) => ({
        default: module.ChartStatsSummarySubHeader,
    })),
);
const SidebarChartHeaderSection = lazyProfileComponent(
    'SidebarChartHeaderSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/Chart/Header/SidebarChartHeaderSection'),
);
const ChartDashboardsTab = lazyProfileComponent('ChartDashboardsTab', () =>
    import('@app/entityV2/shared/tabs/Entity/ChartDashboardsTab').then((module) => ({
        default: module.ChartDashboardsTab,
    })),
);
const InputFieldsTab = lazyProfileComponent('InputFieldsTab', () =>
    import('@app/entityV2/shared/tabs/Entity/InputFieldsTab').then((module) => ({
        default: module.InputFieldsTab,
    })),
);

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

    getEntityName = () => i18next.t('entity.types:chart.name');

    getCollectionName = () => i18next.t('entity.types:chart.namePlural');

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
            tabs={this.getProfileTabs()}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    getProfileTabs = (): EntityTab[] => {
        const showSummaryTab = useShowAssetSummaryPage();

        return [
            {
                name: i18next.t('entity.types:tab.summary'),
                component: showSummaryTab ? SummaryTab : ChartSummaryTab,
                icon: SUMMARY_TAB_ICON,
                display: showSummaryTab
                    ? undefined
                    : {
                          visible: (_, chart: GetChartQuery) =>
                              !!chart?.chart?.subTypes?.typeNames?.includes(SubType.TableauWorksheet) ||
                              !!chart?.chart?.subTypes?.typeNames?.includes(SubType.Looker) ||
                              chart?.chart?.platform?.name === MODE,
                          enabled: () => true,
                      },
            },
            ...(!showSummaryTab
                ? [
                      {
                          name: i18next.t('entity.types:tab.documentation'),
                          component: DocumentationTab,
                          icon: File,
                      },
                  ]
                : []),
            {
                name: i18next.t('entity.types:chart.fieldsTab'),
                component: InputFieldsTab,
                icon: Layout,
                display: {
                    visible: (_, chart: GetChartQuery) => (chart?.chart?.inputFields?.fields?.length || 0) > 0,
                    enabled: (_, chart: GetChartQuery) => (chart?.chart?.inputFields?.fields?.length || 0) > 0,
                },
            },
            {
                name: i18next.t('common.actions:preview'),
                component: EmbedTab,
                icon: Eye,
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
                name: i18next.t('entity.types:tab.lineage'),
                component: LineageTab,
                icon: TreeStructure,
                properties: {
                    defaultDirection: LineageDirection.Upstream,
                },
                supportsFullsize: true,
            },
            {
                name: i18next.t('entity.types:tab.properties'),
                component: PropertiesTab,
                icon: ListBullets,
            },
            {
                name: i18next.t('entity.types:dashboard.namePlural'),
                component: ChartDashboardsTab,
                icon: Gauge,
                display: {
                    visible: (_, _1) => true,
                    enabled: (_, chart: GetChartQuery) => (chart?.chart?.dashboards?.total || 0) > 0,
                },
            },
            {
                name: i18next.t('entity.types:tab.incidents'),
                getCount: (_, chart, loading) => {
                    return !loading ? chart?.chart?.activeIncidents?.total : undefined;
                },
                icon: Warning,
                component: IncidentTab,
            },
        ];
    };

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
            name: i18next.t('entity.types:tab.lineage'),
            component: LineageTab,
            description: i18next.t('entity.types:sidebar.lineageDescription'),
            icon: TreeStructure,
            properties: {
                actionType: SidebarTitleActionType.LineageExplore,
            },
        },
        {
            name: i18next.t('entity.types:tab.properties'),
            component: PropertiesTab,
            description: i18next.t('entity.types:sidebar.propertiesDescription'),
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
                createdMs={this.createdTime(data)}
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

    createdTime = (data: Chart) => {
        return data?.properties?.created?.time || data?.info?.created?.time;
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

    getPlatformProperties = (data: Chart) => {
        return data?.platform;
    };
}
