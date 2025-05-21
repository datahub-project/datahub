import { DatabaseFilled, DatabaseOutlined } from '@ant-design/icons';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entity/Entity';
import { Preview } from '@app/entity/dataset/preview/Preview';
import { OperationsTab } from '@app/entity/dataset/profile/OperationsTab';
import { DatasetStatsSummarySubHeader } from '@app/entity/dataset/profile/stats/stats/DatasetStatsSummarySubHeader';
import { getLastUpdatedMs } from '@app/entity/dataset/shared/utils';
import { EntityMenuItems } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { EntityProfile } from '@app/entity/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entity/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import DataProductSection from '@app/entity/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarViewDefinitionSection } from '@app/entity/shared/containers/profile/sidebar/Dataset/View/SidebarViewDefinitionSection';
import { SidebarDomainSection } from '@app/entity/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entity/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarSiblingsSection } from '@app/entity/shared/containers/profile/sidebar/SidebarSiblingsSection';
import { SidebarTagsSection } from '@app/entity/shared/containers/profile/sidebar/SidebarTagsSection';
import SidebarStructuredPropsSection from '@app/entity/shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';
import { getDataForEntityType } from '@app/entity/shared/containers/profile/utils';
import EmbeddedProfile from '@app/entity/shared/embed/EmbeddedProfile';
import AccessManagement from '@app/entity/shared/tabs/Dataset/AccessManagement/AccessManagement';
import { GovernanceTab } from '@app/entity/shared/tabs/Dataset/Governance/GovernanceTab';
import QueriesTab from '@app/entity/shared/tabs/Dataset/Queries/QueriesTab';
import { RelationshipsTab } from '@app/entity/shared/tabs/Dataset/Relationship/RelationshipsTab';
import { SchemaTab } from '@app/entity/shared/tabs/Dataset/Schema/SchemaTab';
import StatsTab from '@app/entity/shared/tabs/Dataset/Stats/StatsTab';
import { ValidationsTab } from '@app/entity/shared/tabs/Dataset/Validations/ValidationsTab';
import ViewDefinitionTab from '@app/entity/shared/tabs/Dataset/View/ViewDefinitionTab';
import { DocumentationTab } from '@app/entity/shared/tabs/Documentation/DocumentationTab';
import { EmbedTab } from '@app/entity/shared/tabs/Embed/EmbedTab';
import { IncidentTab } from '@app/entity/shared/tabs/Incident/IncidentTab';
import { LineageTab } from '@app/entity/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entity/shared/tabs/Properties/PropertiesTab';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { getDataProduct } from '@app/entity/shared/utils';
import { MatchedFieldList } from '@app/search/matches/MatchedFieldList';
import { matchedFieldPathsRenderer } from '@app/search/matches/matchedFieldPathsRenderer';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useAppConfig } from '@app/useAppConfig';

import { GetDatasetQuery, useGetDatasetQuery, useUpdateDatasetMutation } from '@graphql/dataset.generated';
import { Dataset, DatasetProperties, EntityType, OwnershipType, SearchResult } from '@types';

const SUBTYPES = {
    VIEW: 'view',
};

/**
 * Definition of the DataHub Dataset entity.
 */
export class DatasetEntity implements Entity<Dataset> {
    type: EntityType = EntityType.Dataset;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DatabaseOutlined style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DatabaseFilled style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
        }

        return (
            <DatabaseOutlined
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

    getPathName = () => 'dataset';

    getGraphName = () => 'dataset';

    getEntityName = () => 'Dataset';

    getCollectionName = () => 'Datasets';

    useEntityQuery = useGetDatasetQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Dataset}
            useEntityQuery={this.useEntityQuery}
            useUpdateQuery={useUpdateDatasetMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={new Set([EntityMenuItems.UPDATE_DEPRECATION, EntityMenuItems.RAISE_INCIDENT])}
            subHeader={{
                component: DatasetStatsSummarySubHeader,
            }}
            tabs={[
                {
                    name: 'Schema',
                    component: SchemaTab,
                },
                {
                    name: 'Relationships',
                    component: RelationshipsTab,
                    display: {
                        visible: (_, _1) => false,
                        enabled: (_, _2) => false,
                    },
                },
                {
                    name: 'View Definition',
                    component: ViewDefinitionTab,
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
                },
                {
                    name: 'Preview',
                    component: EmbedTab,
                    display: {
                        visible: (_, dataset: GetDatasetQuery) => !!dataset?.dataset?.embed?.renderUrl,
                        enabled: (_, dataset: GetDatasetQuery) => !!dataset?.dataset?.embed?.renderUrl,
                    },
                },
                {
                    name: 'Lineage',
                    component: LineageTab,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
                {
                    name: 'Queries',
                    component: QueriesTab,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, _2) => true,
                    },
                },
                {
                    name: 'Stats',
                    component: StatsTab,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, dataset: GetDatasetQuery) =>
                            (dataset?.dataset?.datasetProfiles?.length || 0) > 0 ||
                            (dataset?.dataset?.usageStats?.buckets?.length || 0) > 0 ||
                            (dataset?.dataset?.operations?.length || 0) > 0,
                    },
                },
                {
                    name: 'Quality',
                    component: ValidationsTab,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, dataset: GetDatasetQuery) => {
                            return (dataset?.dataset?.assertions?.total || 0) > 0;
                        },
                    },
                },
                {
                    name: 'Governance',
                    component: GovernanceTab,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, dataset: GetDatasetQuery) => {
                            return dataset?.dataset?.testResults !== null;
                        },
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
                    name: 'Access Management',
                    component: AccessManagement,
                    display: {
                        visible: (_, _1) => this.appconfig().config.featureFlags.showAccessManagement,
                        enabled: (_, dataset: GetDatasetQuery) => {
                            const accessAspect = dataset?.dataset?.access;
                            const rolesList = accessAspect?.roles;
                            return !!accessAspect && !!rolesList && rolesList.length > 0;
                        },
                    },
                },
                {
                    name: 'Incidents',
                    component: IncidentTab,
                    getDynamicName: (_, dataset) => {
                        const activeIncidentCount = dataset?.dataset?.activeIncidents?.total;
                        return `Incidents${(activeIncidentCount && ` (${activeIncidentCount})`) || ''}`;
                    },
                },
            ]}
            sidebarSections={this.getSidebarSections()}
            isNameEditable
        />
    );

    getSidebarSections = () => [
        {
            component: SidebarAboutSection,
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
                visible: (_, dataset: GetDatasetQuery) => (dataset?.dataset?.siblingsSearch?.total || 0) > 0,
            },
        },
        {
            component: SidebarViewDefinitionSection,
            display: {
                visible: (_, dataset: GetDatasetQuery) => !!dataset?.dataset?.viewProperties?.logic,
            },
        },
        {
            component: SidebarTagsSection,
            properties: {
                hasTags: true,
                hasTerms: true,
            },
        },
        {
            component: SidebarDomainSection,
        },
        {
            component: DataProductSection,
        },
        {
            component: SidebarStructuredPropsSection,
        },
        // TODO: Add back once entity-level recommendations are complete.
        // {
        //    component: SidebarRecommendationsSection,
        // },
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
                name={data.editableProperties?.name || data.properties?.name || data.name}
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
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Dataset;
        const genericProperties = this.getGenericEntityProperties(data);

        return (
            <Preview
                urn={data.urn}
                name={data.editableProperties?.name || data.properties?.name || data.name}
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
                sizeInBytes={(data as any).lastProfile?.length && (data as any).lastProfile[0].sizeInBytes}
                lastUpdatedMs={getLastUpdatedMs(data.properties, (data as any)?.lastOperation)}
                health={data.health}
                degree={(result as any).degree}
                paths={(result as any).paths}
            />
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
        return data?.editableProperties?.name || data?.properties?.name || data.name || data.urn;
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
        ]);
    };

    renderEmbeddedProfile = (urn: string) => (
        <EmbeddedProfile
            urn={urn}
            entityType={EntityType.Dataset}
            useEntityQuery={this.useEntityQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
        />
    );
}
