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
import { DBT_URN } from '@app/ingest/source/builder/constants';
import ViewComfyOutlinedIcon from '@mui/icons-material/ViewComfyOutlined';
import { GovernanceTab } from '@src/app/entity/shared/tabs/Dataset/Governance/GovernanceTab';
import * as React from 'react';
import { GetDatasetQuery, useGetDatasetQuery, useUpdateDatasetMutation } from '../../../graphql/dataset.generated';
import GovernMenuIcon from '../../../images/governMenuIcon.svg?react';
import { Dataset, DatasetProperties, EntityType, FeatureFlagsConfig, SearchResult } from '../../../types.generated';
import { GenericEntityProperties } from '../../entity/shared/types';
import { MatchedFieldList } from '../../searchV2/matches/MatchedFieldList';
import { matchedFieldPathsRenderer } from '../../searchV2/matches/matchedFieldPathsRenderer';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import { useAppConfig } from '../../useAppConfig';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import { SubType, TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import SidebarDatasetHeaderSection from '../shared/containers/profile/sidebar/Dataset/Header/SidebarDatasetHeaderSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import SidebarLineageSection from '../shared/containers/profile/sidebar/Lineage/SidebarLineageSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarQueryOperationsSection from '../shared/containers/profile/sidebar/Query/SidebarQueryOperationsSection';
import SidebarEntityHeader from '../shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '../shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarDatasetViewDefinitionSection } from '../shared/containers/profile/sidebar/SidebarLogicSection';
import { SidebarSiblingsSection } from '../shared/containers/profile/sidebar/SidebarSiblingsSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '../shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import EmbeddedProfile from '../shared/embed/EmbeddedProfile';
import SidebarNotesSection from '../shared/sidebarSection/SidebarNotesSection';
import SidebarStructuredProperties from '../shared/sidebarSection/SidebarStructuredProperties';
import AccessManagement from '../shared/tabs/Dataset/AccessManagement/AccessManagement';
import QueriesTab from '../shared/tabs/Dataset/Queries/QueriesTab';
import { SchemaTab } from '../shared/tabs/Dataset/Schema/SchemaTab';
import { AcrylValidationsTab } from '../shared/tabs/Dataset/Validations/AcrylValidationsTab';
import ViewDefinitionTab from '../shared/tabs/Dataset/View/ViewDefinitionTab';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { EmbedTab } from '../shared/tabs/Embed/EmbedTab';
import ColumnTabNameHeader from '../shared/tabs/Entity/ColumnTabNameHeader';
import TabNameWithCount from '../shared/tabs/Entity/TabNameWithCount';
import { IncidentTab } from '../shared/tabs/Incident/IncidentTab';
import { LineageTab } from '../shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { SidebarTitleActionType, getDataProduct, getDatasetLastUpdatedMs, isOutputPort } from '../shared/utils';
import { Preview } from './preview/Preview';
import { OperationsTab } from './profile/OperationsTab';
import { DatasetStatsSummarySubHeader } from './profile/stats/stats/DatasetStatsSummarySubHeader';
import StatsTab from '../shared/tabs/Dataset/Stats/StatsTab';

const SUBTYPES = {
    VIEW: 'view',
};

const headerDropdownItems = new Set([
    EntityMenuItems.EXTERNAL_URL,
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
        if (styleType === IconStyleType.TAB_VIEW) {
            return <ViewComfyOutlinedIcon className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <ViewComfyOutlinedIcon
                    className={TYPE_ICON_CLASS_NAME}
                    style={{ fontSize, color: color || '#B37FEB' }}
                />
            );
        }

        if (styleType === IconStyleType.SVG) {
            return <path d="M2 4v16h20V4zm2 2h16v5H4zm0 12v-5h4v5zm6 0v-5h10v5z" />;
        }

        return (
            <ViewComfyOutlinedIcon
                className={TYPE_ICON_CLASS_NAME}
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
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
            tabs={[
                {
                    name: 'Columns',
                    component: SchemaTab,
                    icon: LayoutOutlined,
                    getDynamicName: ColumnTabNameHeader,
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
                    name: 'Properties',
                    component: PropertiesTab,
                    icon: UnorderedListOutlined,
                    getDynamicName: (_, dataset: GetDatasetQuery, loading) => {
                        const customPropertiesCount = dataset?.dataset?.properties?.customProperties?.length || 0;
                        const structuredPropertiesCount =
                            dataset?.dataset?.structuredProperties?.properties?.length || 0;
                        const propertiesCount = customPropertiesCount + structuredPropertiesCount;
                        return <TabNameWithCount name="Properties" count={propertiesCount} loading={loading} />;
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
                    component: StatsTab,
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
                    name: 'Quality',
                    component: AcrylValidationsTab, // Use SaaS specific Validations Tab.
                    icon: CheckCircleOutlined,
                },
                {
                    name: 'Governance',
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
                    name: 'Access Management',
                    component: AccessManagement,
                    icon: UnlockOutlined,
                    display: {
                        visible: (_, _1) => this.appconfig().config.featureFlags.showAccessManagement,
                        enabled: (_, _2) => true,
                    },
                },
                {
                    name: 'Incidents',
                    icon: WarningOutlined,
                    component: IncidentTab,
                    getDynamicName: (_, dataset, loading) => {
                        const activeIncidentCount = dataset?.dataset?.activeIncidents?.total;
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
        { component: SidebarDatasetHeaderSection },
        { component: SidebarAboutSection },
        { component: SidebarNotesSection },
        { component: SidebarLineageSection },
        { component: SidebarOwnerSection },
        { component: SidebarDomainSection },
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
            icon: PartitionOutlined,
            properties: {
                actionType: SidebarTitleActionType.LineageExplore,
            },
        },
        {
            name: 'Columns',
            component: SchemaTab,
            description: "View this data asset's columns",
            icon: LayoutOutlined,
            properties: {
                fullHeight: true,
            },
        },
        {
            name: 'Properties',
            component: PropertiesTab,
            description: 'View additional properties about this asset',
            icon: UnorderedListOutlined,
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
                subtype={data.subTypes?.typeNames?.[0]}
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
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
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
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                deprecation={data.deprecation}
                glossaryTerms={data.glossaryTerms}
                subtype={data.subTypes?.typeNames?.[0]}
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
            subtype: entity?.subTypes?.typeNames?.[0] || undefined,
            icon: entity?.platform?.properties?.logoUrl || undefined,
            platform: entity?.platform,
            health: entity?.health || undefined,
            deprecation: entity?.deprecation,
        };
    };

    displayName = (data: Dataset) => {
        return data?.properties?.name || data.name || data.urn;
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
