import {
    CheckCircleOutlined,
    CodeOutlined,
    ConsoleSqlOutlined,
    EyeOutlined,
    FileOutlined,
    FundOutlined,
    LayoutOutlined,
    PartitionOutlined,
    SyncOutlined,
    TableOutlined,
    UnlockOutlined,
    UnorderedListOutlined,
    WarningOutlined,
} from '@ant-design/icons';
import * as React from 'react';
import { GetDatasetQuery, useGetDatasetQuery, useUpdateDatasetMutation } from '../../../graphql/dataset.generated';
import { useGetEntityLineageQuery } from '../../../graphql/lineage.generated';
import { Dataset, DatasetProperties, EntityType, OwnershipType, SearchResult } from '../../../types.generated';
import { MatchedFieldList } from '../../searchV2/matches/MatchedFieldList';
import { matchedFieldPathsRenderer } from '../../searchV2/matches/matchedFieldPathsRenderer';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import { useAppConfig } from '../../useAppConfig';
import { useEntityRegistry } from '../../useEntityRegistry';
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
import { SidebarCompactSchemaSection } from '../shared/containers/profile/sidebar/SidebarCompactSchemaSection';
import { SidebarGlossaryTermsSection } from '../shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarSiblingsSection } from '../shared/containers/profile/sidebar/SidebarSiblingsSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { getDatasetPopularityTier, isValuePresent } from '../shared/containers/profile/sidebar/shared/utils';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import EmbeddedProfile from '../shared/embed/EmbeddedProfile';
import AccessManagement from '../shared/tabs/Dataset/AccessManagement/AccessManagement';
import QueriesTab from '../shared/tabs/Dataset/Queries/QueriesTab';
import { SchemaTab } from '../shared/tabs/Dataset/Schema/SchemaTab';
import StatsTab from '../shared/tabs/Dataset/Stats/StatsTab';
import { AcrylValidationsTab } from '../shared/tabs/Dataset/Validations/AcrylValidationsTab';
import ViewDefinitionTab from '../shared/tabs/Dataset/View/ViewDefinitionTab';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { EmbedTab } from '../shared/tabs/Embed/EmbedTab';
import { IncidentTab } from '../shared/tabs/Incident/IncidentTab';
import { LineageTab } from '../shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { GenericEntityProperties, TabContextType } from '../shared/types';
import { getDataProduct, isOutputPort } from '../shared/utils';
import { Preview } from './preview/Preview';
import { OperationsTab } from './profile/OperationsTab';
import { DatasetStatsSummarySubHeader } from './profile/stats/stats/DatasetStatsSummarySubHeader';

const SUBTYPES = {
    VIEW: 'view',
};

/**
 * Definition of the DataHub Dataset entity.
 */
export class DatasetEntity implements Entity<Dataset> {
    type: EntityType = EntityType.Dataset;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <TableOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <TableOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
        }

        return (
            <TableOutlined
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

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Dataset}
            useEntityQuery={useGetDatasetQuery}
            useUpdateQuery={useUpdateDatasetMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={
                new Set([
                    EntityMenuItems.EXTERNAL_URL,
                    EntityMenuItems.SHARE,
                    EntityMenuItems.SUBSCRIBE,
                    EntityMenuItems.UPDATE_DEPRECATION,
                    EntityMenuItems.RAISE_INCIDENT,
                ])
            }
            subHeader={{
                component: DatasetStatsSummarySubHeader,
            }}
            tabs={[
                {
                    name: 'Columns',
                    component: SchemaTab,
                    icon: LayoutOutlined,
                },
                {
                    name: 'View Definition',
                    component: ViewDefinitionTab,
                    icon: CodeOutlined,
                    display: {
                        visible: (_, dataset: GetDatasetQuery) =>
                            dataset?.dataset?.subTypes?.typeNames
                                ?.map((t) => t.toLocaleLowerCase())
                                .includes(SUBTYPES.VIEW.toLocaleLowerCase()) || false,
                        enabled: (_, dataset: GetDatasetQuery) =>
                            (dataset?.dataset?.viewProperties?.logic && true) || false,
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
                            (dataset?.dataset?.datasetProfiles?.length || 0) > 0 ||
                            (dataset?.dataset?.usageStats?.buckets?.length || 0) > 0 ||
                            (dataset?.dataset?.operations?.length || 0) > 0,
                    },
                },
                {
                    name: 'Validation',
                    component: AcrylValidationsTab, // Use SaaS specific Validations Tab.
                    icon: CheckCircleOutlined,
                },
                {
                    name: 'Operations',
                    component: OperationsTab,
                    icon: SyncOutlined,
                    display: {
                        visible: (_, dataset: GetDatasetQuery) => {
                            return (
                                (dataset?.dataset?.readRuns?.total || 0) + (dataset?.dataset?.writeRuns?.total || 0) > 0
                            );
                        },
                        enabled: (_, dataset: GetDatasetQuery) => {
                            return (
                                (dataset?.dataset?.readRuns?.total || 0) + (dataset?.dataset?.writeRuns?.total || 0) > 0
                            );
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
                },
            ]}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    getSidebarSections = () => [
        {
            component: SidebarDatasetHeaderSection,
        },
        {
            component: SidebarDomainSection,
        },
        {
            component: DataProductSection,
        },
        {
            component: SidebarAboutSection,
        },
        {
            component: SidebarLineageSection,
        },
        {
            component: SidebarOwnerSection,
            properties: {
                defaultOwnerType: OwnershipType.TechnicalOwner,
            },
        },
        {
            component: SidebarSiblingsSection,
            display: {
                visible: (_, dataset: GetDatasetQuery) => (dataset?.dataset?.siblings?.siblings?.length || 0) > 0,
            },
        },
        {
            component: SidebarTagsSection,
        },
        {
            component: SidebarGlossaryTermsSection,
        },
        {
            component: SidebarCompactSchemaSection,
            display: {
                visible: (_, __, contextType) =>
                    contextType === TabContextType.SEARCH_SIDEBAR || contextType === TabContextType.LINEAGE_SIDEBAR,
            },
        },
        // TODO: Add back once entity-level recommendations are complete.
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

    getOverridePropertiesFromEntity = (dataset?: Dataset | null): GenericEntityProperties => {
        // if dataset has subTypes filled out, pick the most specific subtype and return it
        const subTypes = dataset?.subTypes;

        const extendedProperties: DatasetProperties | undefined | null = dataset?.properties && {
            ...dataset?.properties,
            qualifiedName: dataset?.properties?.qualifiedName || this.displayName(dataset),
        };
        return {
            name: dataset && this.displayName(dataset),
            externalUrl: dataset?.properties?.externalUrl,
            entityTypeOverride: subTypes ? capitalizeFirstLetterOnly(subTypes.typeNames?.[0]) : '',
            properties: extendedProperties,
        };
    };

    renderPreview = (_: PreviewType, data: Dataset) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || data.name}
                origin={data.origin}
                subtype={data.subTypes?.typeNames?.[0]}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={
                    data?.platform?.properties?.displayName || capitalizeFirstLetterOnly(data?.platform?.name)
                }
                platformLogo={data.platform.properties?.logoUrl}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
                glossaryTerms={data.glossaryTerms}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                container={data.container}
                externalUrl={data.properties?.externalUrl}
                health={data.health}
                tier={
                    (isValuePresent(data?.statsSummary?.queryCountPercentileLast30Days) &&
                        isValuePresent(data?.statsSummary?.uniqueUserPercentileLast30Days) &&
                        getDatasetPopularityTier(
                            data.statsSummary?.queryCountPercentileLast30Days,
                            data.statsSummary?.uniqueUserPercentileLast30Days,
                        )) ||
                    undefined
                }
                upstreamTotal={(data as any)?.upstream?.total}
                downstreamTotal={(data as any)?.downstream?.total}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Dataset;
        const genericProperties = this.getGenericEntityProperties(data);

        console.log(data);

        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || data.name}
                origin={data.origin}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={
                    data?.platform?.properties?.displayName || capitalizeFirstLetterOnly(data?.platform?.name)
                }
                platformLogo={data.platform.properties?.logoUrl}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                platformNames={genericProperties?.siblingPlatforms?.map(
                    (platform) => platform.properties?.displayName || capitalizeFirstLetterOnly(platform.name),
                )}
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
                lastUpdatedMs={
                    (data as any).lastOperation?.length && (data as any).lastOperation[0].lastUpdatedTimestamp
                }
                health={data.health}
                degree={(result as any).degree}
                paths={(result as any).paths}
                isOutputPort={isOutputPort(result)}
                tier={
                    (isValuePresent(data?.statsSummary?.queryCountPercentileLast30Days) &&
                        isValuePresent(data?.statsSummary?.uniqueUserPercentileLast30Days) &&
                        getDatasetPopularityTier(
                            data.statsSummary?.queryCountPercentileLast30Days,
                            data.statsSummary?.uniqueUserPercentileLast30Days,
                        )) ||
                    undefined
                }
                upstreamTotal={(data as any)?.upstream?.total}
                downstreamTotal={(data as any)?.downstream?.total}
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
        };
    };

    displayName = (data: Dataset) => {
        return data?.properties?.name || data.name || data.urn;
    };

    platformLogoUrl = (data: Dataset) => {
        return data.platform.properties?.logoUrl || undefined;
    };

    getGenericEntityProperties = (data: Dataset) => {
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

    renderEmbeddedProfile = (urn: string) => (
        <EmbeddedProfile
            urn={urn}
            entityType={EntityType.Dataset}
            useEntityQuery={useGetDatasetQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
        />
    );

    renderSummaryRows = (dataset?: Dataset | null): JSX.Element | undefined => {
        const entityRegistry = useEntityRegistry();
        const isTableauDataSource =
            dataset?.subTypes?.typeNames?.includes(SubType.TableauPublishedDataSource) ||
            dataset?.subTypes?.typeNames?.includes(SubType.TableauEmbeddedDataSource);

        const { data } = useGetEntityLineageQuery({
            skip: !dataset?.urn || !isTableauDataSource,
            variables: {
                urn: dataset?.urn || '',
                showColumns: false,
                excludeDownstream: true,
            },
            fetchPolicy: 'cache-first',
        });

        if (isTableauDataSource && data?.entity) {
            const parents = entityRegistry.getLineageVizConfig(data.entity.type, data.entity)?.upstreamChildren;
            const parent = parents?.[0];
            if (parent) {
                const parentProperties = entityRegistry.getGenericEntityProperties(parent?.type, parent?.entity);
                const platformIcon = parentProperties?.platform?.properties?.logoUrl;
                const platformName = entityRegistry.getDisplayName(EntityType.DataPlatform, parentProperties?.platform);
                const name = entityRegistry.getDisplayName(parent?.type, parent?.entity);
                return (
                    <>
                        Connects to: <br />
                        {platformIcon && <img src={platformIcon} alt={platformName} />}
                        {name}
                        {parents?.length > 1 && (
                            <>
                                <br /> and {parents.length - 1} more
                            </>
                        )}
                    </>
                );
            }
        }
        return undefined;
    };
}
