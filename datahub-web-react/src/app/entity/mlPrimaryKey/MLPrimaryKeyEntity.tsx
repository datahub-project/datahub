import * as React from 'react';
import { DotChartOutlined } from '@ant-design/icons';
import { MlPrimaryKey, EntityType, SearchResult, OwnershipType } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { GenericEntityProperties } from '../shared/types';
import { useGetMlPrimaryKeyQuery } from '../../../graphql/mlPrimaryKey.generated';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { FeatureTableTab } from '../shared/tabs/ML/MlPrimaryKeyFeatureTableTab';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { LineageTab } from '../shared/tabs/Lineage/LineageTab';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { getDataProduct } from '../shared/utils';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import SidebarStructuredPropsSection from '../shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';

/**
 * Definition of the DataHub MLPrimaryKey entity.
 */
export class MLPrimaryKeyEntity implements Entity<MlPrimaryKey> {
    type: EntityType = EntityType.MlprimaryKey;

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

    getGraphName = () => 'mlPrimaryKey';

    getPathName = () => 'mlPrimaryKeys';

    getEntityName = () => 'ML Primary Key';

    getCollectionName = () => 'ML Primary Keys';

    getOverridePropertiesFromEntity = (key?: MlPrimaryKey | null): GenericEntityProperties => {
        return {
            // eslint-disable-next-line
            platform: key?.['featureTables']?.relationships?.[0]?.entity?.platform,
        };
    };

    useEntityQuery = useGetMlPrimaryKeyQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.MlprimaryKey}
            useEntityQuery={useGetMlPrimaryKeyQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
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

    renderPreview = (_: PreviewType, data: MlPrimaryKey) => {
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
        const data = result.entity as MlPrimaryKey;
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
                platform={platform}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                degree={(result as any).degree}
                paths={(result as any).paths}
            />
        );
    };

    displayName = (data: MlPrimaryKey) => {
        return data.name || data.urn;
    };

    getGenericEntityProperties = (mlPrimaryKey: MlPrimaryKey) => {
        return getDataForEntityType({
            data: mlPrimaryKey,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    getLineageVizConfig = (entity: MlPrimaryKey) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.MlprimaryKey,
            // eslint-disable-next-line
            icon: entity?.['featureTables']?.relationships?.[0]?.entity?.platform?.properties?.logoUrl || undefined,
            // eslint-disable-next-line
            platform: entity?.['featureTables']?.relationships?.[0]?.entity?.platform?.name,
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
