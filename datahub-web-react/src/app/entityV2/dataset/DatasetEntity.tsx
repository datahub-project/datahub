import {
    CheckCircleOutlined,
    CodeOutlined,
    ConsoleSqlOutlined,
    EyeOutlined,
    FileOutlined,
    FundOutlined,
    LayoutOutlined,
    PartitionOutlined,
    UnlockOutlined,
    UnorderedListOutlined,
    WarningOutlined,
} from '@ant-design/icons';
import { Columns, ListBullets, Table, TreeStructure } from '@phosphor-icons/react';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { GOVERNANCE_TAB_NAME, QUALITY_TAB_NAME } from '@app/entityV2/dataset/constants';
import { Preview } from '@app/entityV2/dataset/preview/Preview';
import { OperationsTab } from '@app/entityV2/dataset/profile/OperationsTab';
import { DatasetStatsSummarySubHeader } from '@app/entityV2/dataset/profile/stats/stats/DatasetStatsSummarySubHeader';
import { useGetColumnTabCount } from '@app/entityV2/dataset/profile/useGetColumnTabCount';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { SubType, TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarApplicationSection } from '@app/entityV2/shared/containers/profile/sidebar/Applications/SidebarApplicationSection';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import SidebarDatasetHeaderSection from '@app/entityV2/shared/containers/profile/sidebar/Dataset/Header/SidebarDatasetHeaderSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import SidebarLineageSection from '@app/entityV2/shared/containers/profile/sidebar/Lineage/SidebarLineageSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarQueryOperationsSection from '@app/entityV2/shared/containers/profile/sidebar/Query/SidebarQueryOperationsSection';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarDatasetViewDefinitionSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarLogicSection';
import { SidebarSiblingsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSiblingsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import EmbeddedProfile from '@app/entityV2/shared/embed/EmbeddedProfile';
import SidebarNotesSection from '@app/entityV2/shared/sidebarSection/SidebarNotesSection';
import SidebarStructuredProperties from '@app/entityV2/shared/sidebarSection/SidebarStructuredProperties';
import { SUMMARY_TAB_ICON } from '@app/entityV2/shared/summary/HeaderComponents';
import AccessManagement from '@app/entityV2/shared/tabs/Dataset/AccessManagement/AccessManagement';
import QueriesTab from '@app/entityV2/shared/tabs/Dataset/Queries/QueriesTab';
import { SchemaTab } from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaTab';
import StatsTabWrapper from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabWrapper';
import { AcrylValidationsTab } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylValidationsTab';
import ViewDefinitionTab from '@app/entityV2/shared/tabs/Dataset/View/ViewDefinitionTab';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { EmbedTab } from '@app/entityV2/shared/tabs/Embed/EmbedTab';
import { IncidentTab } from '@app/entityV2/shared/tabs/Incident/IncidentTab';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { EntityTab } from '@app/entityV2/shared/types';
import {
    SidebarTitleActionType,
    getDatasetLastUpdatedMs,
    getFirstSubType,
    isOutputPort,
} from '@app/entityV2/shared/utils';
import SummaryTab from '@app/entityV2/summary/SummaryTab';
import { useShowDatasetSummaryPage } from '@app/entityV2/summary/useShowDatasetSummaryPage';
import { DBT_URN } from '@app/ingest/source/builder/constants';
import { MatchedFieldList } from '@app/searchV2/matches/MatchedFieldList';
import { matchedFieldPathsRenderer } from '@app/searchV2/matches/matchedFieldPathsRenderer';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useAppConfig } from '@app/useAppConfig';
import { GovernanceTab } from '@src/app/entity/shared/tabs/Dataset/Governance/GovernanceTab';

import { GetDatasetQuery, useGetDatasetQuery, useUpdateDatasetMutation } from '@graphql/dataset.generated';
import { Dataset, DatasetProperties, EntityType, FeatureFlagsConfig, SearchResult } from '@types';

import GovernMenuIcon from '@images/governMenuIcon.svg?react';

const SUBTYPES = {
    VIEW: 'view',
};

const headerDropdownItems = new Set([
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.RAISE_INCIDENT,
    EntityMenuItems.ANNOUNCE,
    EntityMenuItems.LINK_VERSION,
]);

/**
 * Definition of the DataHub Dataset entity.
 */
export class DatasetEntity implements Entity<Dataset> {
    type: EntityType = EntityType.Dataset;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.SVG) {
            return <path d="M2 4v16h20V4zm2 2h16v5H4zm0 12v-5h4v5zm6 0v-5h10v5z" fill="currentColor" />;
        }
        return (
            <Table
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
            />
        );
    };

    isSearchEnabled = () => true;

    appconfig = useAppConfig;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'dataset';

    getPathName = () => this.getGraphName();

    getEntityName = () => 'Dataset';

    getCollectionName = () => 'Datasets';

    useEntityQuery = useGetDatasetQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Dataset}
            useEntityQuery={useGetDatasetQuery}
            useUpdateQuery={useUpdateDatasetMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            subHeader={{
                component: DatasetStatsSummarySubHeader,
            }}
            tabs={this.getProfileTabs()}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    getSidebarSections = () => [
        { component: SidebarEntityHeader },
        { component: SidebarDatasetHeaderSection },
        { component: SidebarAboutSection },
        { component: SidebarNotesSection },
        { component: SidebarLineageSection },
        { component: SidebarOwnerSection },
        { component: SidebarDomainSection },
        { component: SidebarApplicationSection },
        { component: DataProductSection },
        { component: SidebarTagsSection },
        { component: SidebarGlossaryTermsSection },
        {
            component: SidebarSiblingsSection,
            display: {
                visible: (_, dataset: GetDatasetQuery) => !!dataset?.dataset?.siblingsSearch?.total,
            },
        },
        { component: SidebarDatasetViewDefinitionSection },
        { component: SidebarQueryOperationsSection },
        { component: SidebarStructuredProperties },
        { component: StatusSection },
        // {
        //    component: SidebarRecommendationsSection,
        // },
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
            name: 'Columns',
            component: SchemaTab,
            description: "View this data asset's columns",
            icon: Columns,
            properties: {
                fullHeight: true,
            },
        },
        {
            name: 'Properties',
            component: PropertiesTab,
            description: 'View additional properties about this asset',
            icon: ListBullets,
        },
    ];

    #shouldMergeInLineage(dataset?: Dataset | null, flags?: FeatureFlagsConfig): boolean {
        // Lineage query must include platform and typeNames on dataset and its sibling
        return (
            !!flags?.hideDbtSourceInLineage &&
            dataset?.platform?.urn === DBT_URN &&
            !!dataset?.subTypes?.typeNames?.includes(SubType.DbtSource)
        );
    }

    getOverridePropertiesFromEntity = (
        dataset?: Dataset | null,
        flags?: FeatureFlagsConfig,
    ): GenericEntityProperties => {
        // if dataset has subTypes filled out, pick the most specific subtype and return it
        const subTypes = dataset?.subTypes;

        const extendedProperties: DatasetProperties | undefined | null = dataset?.properties && {
            ...dataset?.properties,
            qualifiedName: dataset?.properties?.qualifiedName || this.displayName(dataset),
        };

        const firstSibling = dataset?.siblingsSearch?.searchResults?.[0]?.entity as Dataset | undefined;
        const isReplacedBySibling = this.#shouldMergeInLineage(dataset, flags);
        const isSiblingHidden = this.#shouldMergeInLineage(firstSibling, flags);

        const lineageUrn = isReplacedBySibling ? firstSibling?.urn : undefined;
        let lineageSiblingIcon: string | undefined;
        if (isReplacedBySibling) {
            // Swap lineage urn and show as merged with sibling, extra icon is the original entity icon
            lineageSiblingIcon = dataset?.platform?.properties?.logoUrl ?? undefined;
        } else if (isSiblingHidden) {
            // Same lineage urn but show as merged with sibling, extra icon is the sibling's icon
            lineageSiblingIcon = firstSibling?.platform?.properties?.logoUrl ?? undefined;
        }
        return {
            name: dataset && this.displayName(dataset),
            externalUrl: dataset?.properties?.externalUrl,
            entityTypeOverride: subTypes ? capitalizeFirstLetterOnly(subTypes.typeNames?.[0]) : '',
            properties: extendedProperties,
            lineageUrn,
            lineageSiblingIcon,
        };
    };

    getProfileTabs = (): EntityTab[] => {
        const showSummaryTab = useShowDatasetSummaryPage();
        return [
            ...(showSummaryTab
                ? [
                      {
                          name: 'Summary',
                          component: SummaryTab,
                          icon: SUMMARY_TAB_ICON,
                      },
                  ]
                : []),
            {
                name: 'Columns',
                component: SchemaTab,
                icon: LayoutOutlined,
                getCount: useGetColumnTabCount,
            },
            {
                name: 'View Definition',
                component: ViewDefinitionTab,
                icon: CodeOutlined,
                display: {
                    visible: (_, dataset: GetDatasetQuery) =>
                        !!dataset?.dataset?.viewProperties?.logic ||
                        !!dataset?.dataset?.subTypes?.typeNames
                            ?.map((t) => t.toLocaleLowerCase())
                            .includes(SUBTYPES.VIEW.toLocaleLowerCase()),
                    enabled: (_, dataset: GetDatasetQuery) => !!dataset?.dataset?.viewProperties?.logic,
                },
            },
            ...(!showSummaryTab
                ? [
                      {
                          name: 'Documentation',
                          component: DocumentationTab,
                          icon: FileOutlined,
                      },
                  ]
                : []),
            {
                name: 'Preview',
                component: EmbedTab,
                icon: EyeOutlined,
                display: {
                    visible: (_, dataset: GetDatasetQuery) => !!dataset?.dataset?.embed?.renderUrl,
                    enabled: (_, dataset: GetDatasetQuery) => !!dataset?.dataset?.embed?.renderUrl,
                },
            },
            {
                name: 'Lineage',
                component: LineageTab,
                icon: PartitionOutlined,
            },
            {
                name: 'Access',
                component: AccessManagement,
                icon: UnlockOutlined,
                display: {
                    visible: (_, _1) => this.appconfig().config.featureFlags.showAccessManagement,
                    enabled: (_, _2) => true,
                },
            },
            {
                name: 'Properties',
                component: PropertiesTab,
                icon: UnorderedListOutlined,
                getCount: (_, dataset: GetDatasetQuery) => {
                    const customPropertiesCount = dataset?.dataset?.properties?.customProperties?.length || 0;
                    const structuredPropertiesCount = dataset?.dataset?.structuredProperties?.properties?.length || 0;
                    const propertiesCount = customPropertiesCount + structuredPropertiesCount;
                    return propertiesCount;
                },
            },
            {
                name: 'Queries',
                component: QueriesTab,
                icon: ConsoleSqlOutlined,
                display: {
                    visible: (_, _1) => true,
                    enabled: (_, _2) => true,
                },
            },
            {
                name: 'Stats',
                component: StatsTabWrapper,
                icon: FundOutlined,
                display: {
                    visible: (_, _1) => true,
                    enabled: (_, dataset: GetDatasetQuery) =>
                        (dataset?.dataset?.latestFullTableProfile?.length || 0) > 0 ||
                        (dataset?.dataset?.latestPartitionProfile?.length || 0) > 0 ||
                        (dataset?.dataset?.usageStats?.buckets?.length || 0) > 0 ||
                        (dataset?.dataset?.operations?.length || 0) > 0,
                },
            },
            {
                name: QUALITY_TAB_NAME,
                component: AcrylValidationsTab, // Use SaaS specific Validations Tab.
                icon: CheckCircleOutlined,
            },
            {
                name: GOVERNANCE_TAB_NAME,
                icon: () => (
                    <span
                        style={{
                            marginRight: 6,
                            verticalAlign: '-0.2em',
                        }}
                    >
                        <GovernMenuIcon width={16} height={16} fill="currentColor" />
                    </span>
                ),
                component: GovernanceTab,
                getCount: (_, dataset) => {
                    const passingTests = dataset?.dataset?.testResults?.passing || [];
                    const failingTests = dataset?.dataset?.testResults?.failing || [];
                    return passingTests.length + failingTests.length;
                },
            },
            {
                name: 'Runs', // TODO: Rename this to DatasetRunsTab.
                component: OperationsTab,
                display: {
                    visible: (_, dataset: GetDatasetQuery) => {
                        return (dataset?.dataset?.runs?.total || 0) > 0;
                    },
                    enabled: (_, dataset: GetDatasetQuery) => {
                        return (dataset?.dataset?.runs?.total || 0) > 0;
                    },
                },
            },
            {
                name: 'Incidents',
                icon: WarningOutlined,
                component: IncidentTab,
                getCount: (_, dataset) => {
                    return dataset?.dataset?.activeIncidents?.total;
                },
            },
        ];
    };

    renderPreview = (previewType: PreviewType, data: Dataset) => {
        const genericProperties = this.getGenericEntityProperties(data);
        const platformNames = genericProperties?.siblingPlatforms?.map(
            (platform) => platform.properties?.displayName || capitalizeFirstLetterOnly(platform.name),
        );
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.properties?.name || data.name}
                origin={data.origin}
                subtype={getFirstSubType(data)}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={
                    data?.platform?.properties?.displayName || capitalizeFirstLetterOnly(data?.platform?.name)
                }
                platformNames={platformNames}
                platformLogo={data.platform.properties?.logoUrl}
                platformLogos={genericProperties?.siblingPlatforms?.map((platform) => platform.properties?.logoUrl)}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
                glossaryTerms={data.glossaryTerms}
                container={data.container}
                externalUrl={data.properties?.externalUrl}
                health={data.health}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
                browsePaths={data.browsePathV2 || undefined}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Dataset;
        const genericProperties = this.getGenericEntityProperties(data);
        const platformNames = genericProperties?.siblingPlatforms?.map(
            (platform) => platform.properties?.displayName || capitalizeFirstLetterOnly(platform.name),
        );

        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.properties?.name || data.name}
                origin={data.origin}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={
                    platformNames?.[0] ||
                    data?.platform?.properties?.displayName ||
                    capitalizeFirstLetterOnly(data?.platform?.name)
                }
                platformLogo={data.platform.properties?.logoUrl}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                platformNames={platformNames}
                platformLogos={genericProperties?.siblingPlatforms?.map((platform) => platform.properties?.logoUrl)}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
                deprecation={data.deprecation}
                glossaryTerms={data.glossaryTerms}
                subtype={getFirstSubType(data)}
                container={data.container}
                parentContainers={data.parentContainers}
                snippet={<MatchedFieldList customFieldRenderer={matchedFieldPathsRenderer} />}
                insights={result.insights}
                externalUrl={data.properties?.externalUrl}
                statsSummary={data.statsSummary}
                rowCount={(data as any).lastProfile?.length && (data as any).lastProfile[0].rowCount}
                columnCount={(data as any).lastProfile?.length && (data as any).lastProfile[0].columnCount}
                lastUpdatedMs={getDatasetLastUpdatedMs(
                    (data as any).properties,
                    (data as any).lastOperation?.length && (data as any).lastOperation[0],
                )}
                health={data.health}
                degree={(result as any).degree}
                paths={(result as any).paths}
                isOutputPort={isOutputPort(result)}
                headerDropdownItems={headerDropdownItems}
                browsePaths={data.browsePathV2 || undefined}
                previewType={PreviewType.SEARCH}
            />
        );
    };

    renderSearchMatches = (_: SearchResult) => {
        return (
            <>
                <MatchedFieldList customFieldRenderer={matchedFieldPathsRenderer} />
            </>
        );
    };

    getLineageVizConfig = (entity: Dataset) => {
        return {
            urn: entity?.urn,
            name: entity?.properties?.name || entity.name,
            expandedName: entity?.properties?.qualifiedName || entity?.properties?.name || entity.name,
            type: EntityType.Dataset,
            subtype: getFirstSubType(entity) || undefined,
            icon: entity?.platform?.properties?.logoUrl || undefined,
            platform: entity?.platform,
            health: entity?.health || undefined,
            deprecation: entity?.deprecation,
        };
    };

    displayName = (data: Dataset) => {
        return data?.editableProperties?.name || data?.properties?.name || data.name || data.urn;
    };

    platformLogoUrl = (data: Dataset) => {
        return data.platform.properties?.logoUrl || undefined;
    };

    getGenericEntityProperties = (data: Dataset, flags?: FeatureFlagsConfig) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
            flags,
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
            entityType={EntityType.Dataset}
            useEntityQuery={useGetDatasetQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
        />
    );
}
