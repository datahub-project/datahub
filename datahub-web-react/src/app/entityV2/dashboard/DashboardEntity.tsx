import { ChartBar } from '@phosphor-icons/react/dist/csr/ChartBar';
import { Eye } from '@phosphor-icons/react/dist/csr/Eye';
import { File } from '@phosphor-icons/react/dist/csr/File';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { LockOpen } from '@phosphor-icons/react/dist/csr/LockOpen';
import { SquaresFour } from '@phosphor-icons/react/dist/csr/SquaresFour';
import { Table } from '@phosphor-icons/react/dist/csr/Table';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import i18next from 'i18next';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { DashboardPreview } from '@app/entityV2/dashboard/preview/DashboardPreview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/entityData';
import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import {
    AccessManagement,
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
import { LOOKER_URN, MODE_URN } from '@app/ingest/source/builder/constants';
import { matchedInputFieldRenderer } from '@app/search/matches/matchedInputFieldRenderer';
import { MatchedFieldList } from '@app/searchV2/matches/MatchedFieldList';
import { MatchContext } from '@app/searchV2/matches/utils';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useAppConfig } from '@app/useAppConfig';

import { GetDashboardQuery, useGetDashboardQuery, useUpdateDashboardMutation } from '@graphql/dashboard.generated';
import { Dashboard, EntityType, LineageDirection, SearchResult } from '@types';

const DashboardSummaryTab = lazyProfileComponent(
    'DashboardSummaryTab',
    () => import('@app/entityV2/dashboard/summary/DashboardSummaryTab'),
);

const DashboardStatsSummarySubHeader = lazyProfileComponent('DashboardStatsSummarySubHeader', () =>
    import('@app/entityV2/dashboard/profile/DashboardStatsSummarySubHeader').then((module) => ({
        default: module.DashboardStatsSummarySubHeader,
    })),
);
const SidebarDashboardHeaderSection = lazyProfileComponent(
    'SidebarDashboardHeaderSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/Dashboard/Header/SidebarDashboardHeaderSection'),
);
const DashboardChartsTab = lazyProfileComponent('DashboardChartsTab', () =>
    import('@app/entityV2/shared/tabs/Entity/DashboardChartsTab').then((module) => ({
        default: module.DashboardChartsTab,
    })),
);
const DashboardDatasetsTab = lazyProfileComponent('DashboardDatasetsTab', () =>
    import('@app/entityV2/shared/tabs/Entity/DashboardDatasetsTab').then((module) => ({
        default: module.DashboardDatasetsTab,
    })),
);

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

    getEntityName = () => i18next.t('entity.types:dashboard.name');

    getCollectionName = () => i18next.t('entity.types:dashboard.namePlural');

    useEntityQuery = useGetDashboardQuery;

    appconfig = useAppConfig;

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
                component: showSummaryTab ? SummaryTab : DashboardSummaryTab,
                icon: SUMMARY_TAB_ICON,
            },
            {
                name: i18next.t('entity.types:tab.contents'),
                component: DashboardChartsTab,
                icon: SquaresFour,
                display: {
                    visible: (_, dashboard: GetDashboardQuery) =>
                        (dashboard?.dashboard?.charts?.total || 0) > 0 ||
                        (dashboard?.dashboard?.datasets?.total || 0) === 0,
                    enabled: (_, dashboard: GetDashboardQuery) => (dashboard?.dashboard?.charts?.total || 0) > 0,
                },
            },
            {
                name: i18next.t('entity.types:dataset.namePlural'),
                component: DashboardDatasetsTab,
                icon: Table,
                display: {
                    visible: (_, dashboard: GetDashboardQuery) => (dashboard?.dashboard?.datasets?.total || 0) > 0,
                    enabled: (_, dashboard: GetDashboardQuery) => (dashboard?.dashboard?.datasets?.total || 0) > 0,
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
                name: i18next.t('entity.types:shared.accessTab'),
                component: AccessManagement,
                icon: LockOpen,
                display: {
                    visible: (_, _1) => this.appconfig().config.featureFlags.showAccessManagement,
                    enabled: (_, _2) => true,
                },
            },
            {
                name: i18next.t('common.actions:preview'),
                component: EmbedTab,
                icon: Eye,
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
                name: i18next.t('entity.types:tab.incidents'),
                icon: Warning,
                component: IncidentTab,
                getCount: (_, dashboard) => {
                    return dashboard?.dashboard?.activeIncidents?.total;
                },
            },
        ];
    };

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
            name: i18next.t('entity.types:tab.lineage'),
            component: LineageTab,
            description: i18next.t('entity.types:sidebar.lineageDescription'),
            icon: TreeStructure,
            properties: {
                defaultDirection: LineageDirection.Upstream,
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
                createdMs={this.createdTime(data)}
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
                createdMs={this.createdTime(data)}
                snippet={
                    <MatchedFieldList
                        customFieldRenderer={(matchedField) => matchedInputFieldRenderer(matchedField, data)}
                        matchContext={MatchContext.ContainedChart}
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
                matchContext={MatchContext.ContainedChart}
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

    createdTime = (data: Dashboard) => {
        return data?.properties?.created?.time || data?.info?.created?.time;
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

    getPlatformProperties = (data: Dashboard) => {
        return data?.platform;
    };
}
