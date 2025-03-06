import { DotChartOutlined, PartitionOutlined, UnorderedListOutlined } from '@ant-design/icons';
import * as React from 'react';
import { useGetMlPrimaryKeyQuery } from '../../../graphql/mlPrimaryKey.generated';
import { EntityType, MlPrimaryKey, SearchResult } from '../../../types.generated';
import { GenericEntityProperties } from '../../entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '../shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '../shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '../shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import SidebarStructuredProperties from '../shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { LineageTab } from '../shared/tabs/Lineage/LineageTab';
import { FeatureTableTab } from '../shared/tabs/ML/MlPrimaryKeyFeatureTableTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { SidebarTitleActionType, getDataProduct, isOutputPort } from '../shared/utils';
import { Preview } from './preview/Preview';

/**
 * Definition of the DataHub MLPrimaryKey entity.
 */
export class MLPrimaryKeyEntity implements Entity<MlPrimaryKey> {
    type: EntityType = EntityType.MlprimaryKey;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DotChartOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <DotChartOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#9633b9' }} />
            );
        }

        return (
            <DotChartOutlined
                className={TYPE_ICON_CLASS_NAME}
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
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    getSidebarSections = () => [
        {
            component: SidebarEntityHeader,
        },
        {
            component: SidebarAboutSection,
        },
        {
            component: SidebarOwnerSection,
        },
        {
            component: SidebarDomainSection,
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
            icon: PartitionOutlined,
            properties: {
                actionType: SidebarTitleActionType.LineageExplore,
            },
        },
        {
            name: 'Properties',
            component: PropertiesTab,
            description: 'View additional properties about this asset',
            icon: UnorderedListOutlined,
        },
    ];

    renderPreview = (previewType: PreviewType, data: MlPrimaryKey) => {
        const genericProperties = this.getGenericEntityProperties(data);
        // eslint-disable-next-line
        const platform = data?.['featureTables']?.relationships?.[0]?.entity?.platform;
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.name || ''}
                featureNamespace={data.featureNamespace || ''}
                description={data.description}
                owners={data.ownership?.owners}
                platform={platform}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                previewType={previewType}
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
                data={genericProperties}
                name={data.name || ''}
                featureNamespace={data.featureNamespace || ''}
                description={data.description || ''}
                owners={data.ownership?.owners}
                platform={platform}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                degree={(result as any).degree}
                paths={(result as any).paths}
                isOutputPort={isOutputPort(result)}
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
            EntityCapabilityType.LINEAGE,
        ]);
    };
}
