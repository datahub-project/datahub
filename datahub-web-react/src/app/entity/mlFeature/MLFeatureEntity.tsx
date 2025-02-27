import * as React from 'react';
import { DotChartOutlined } from '@ant-design/icons';
import { MlFeature, EntityType, SearchResult, OwnershipType } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { GenericEntityProperties } from '../shared/types';
import { useGetMlFeatureQuery } from '../../../graphql/mlFeature.generated';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { FeatureTableTab } from '../shared/tabs/ML/MlFeatureFeatureTableTab';
import { LineageTab } from '../shared/tabs/Lineage/LineageTab';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityDropdown';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { getDataProduct } from '../shared/utils';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import SidebarStructuredPropsSection from '../shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';
import { IncidentTab } from '../shared/tabs/Incident/IncidentTab';

/**
 * Definition of the DataHub MLFeature entity.
 */
export class MLFeatureEntity implements Entity<MlFeature> {
    type: EntityType = EntityType.Mlfeature;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DotChartOutlined style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DotChartOutlined style={{ fontSize, color: color || '#9633b9' }} />;
        }

        return (
            <DotChartOutlined
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'mlFeature';

    getPathName = () => 'features';

    getEntityName = () => 'Feature';

    getCollectionName = () => 'Features';

    useEntityQuery = useGetMlFeatureQuery;

    getOverridePropertiesFromEntity = (feature?: MlFeature | null): GenericEntityProperties => {
        return {
            // eslint-disable-next-line
            platform: feature?.['featureTables']?.relationships?.[0]?.entity?.platform,
        };
    };

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.Mlfeature}
            useEntityQuery={useGetMlFeatureQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={new Set([EntityMenuItems.UPDATE_DEPRECATION])}
            tabs={[
                {
                    name: 'Feature Tables',
                    component: FeatureTableTab,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
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
                    name: 'Incidents',
                    component: IncidentTab,
                    getDynamicName: (_, mlFeature) => {
                        const activeIncidentCount = mlFeature?.mlFeature?.activeIncidents?.total;
                        return `Incidents${(activeIncidentCount && ` (${activeIncidentCount})`) || ''}`;
                    },
                },
            ]}
            sidebarSections={this.getSidebarSections()}
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
    ];

    renderPreview = (_: PreviewType, data: MlFeature) => {
        const genericProperties = this.getGenericEntityProperties(data);
        // eslint-disable-next-line
        const platform = data?.['featureTables']?.relationships?.[0]?.entity?.platform;
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                featureNamespace={data.featureNamespace || ''}
                description={data.description}
                owners={data.ownership?.owners}
                platform={platform}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlFeature;
        const genericProperties = this.getGenericEntityProperties(data);
        // eslint-disable-next-line
        const platform = data?.['featureTables']?.relationships?.[0]?.entity?.platform;
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                featureNamespace={data.featureNamespace || ''}
                description={data.description || ''}
                owners={data.ownership?.owners}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                platform={platform}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                degree={(result as any).degree}
                paths={(result as any).paths}
            />
        );
    };

    displayName = (data: MlFeature) => {
        return data.name || data.urn;
    };

    getGenericEntityProperties = (mlFeature: MlFeature) => {
        return getDataForEntityType({
            data: mlFeature,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    getLineageVizConfig = (entity: MlFeature) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.Mlfeature,
            // eslint-disable-next-line
            icon: entity?.['featureTables']?.relationships?.[0]?.entity?.platform?.properties?.logoUrl || undefined,
            // eslint-disable-next-line
            platform: entity?.['featureTables']?.relationships?.[0]?.entity?.platform,
        };
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
}
