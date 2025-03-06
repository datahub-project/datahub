import { DotChartOutlined, PartitionOutlined, UnorderedListOutlined, WarningOutlined } from '@ant-design/icons';
import { IncidentTab } from '@app/entity/shared/tabs/Incident/IncidentTab';
import TabNameWithCount from '@app/entityV2/shared/tabs/Entity/TabNameWithCount';
import * as React from 'react';
import { useGetMlFeatureQuery } from '../../../graphql/mlFeature.generated';
import { EntityType, MlFeature, SearchResult } from '../../../types.generated';
import { GenericEntityProperties } from '../../entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
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
import { FeatureTableTab } from '../shared/tabs/ML/MlFeatureFeatureTableTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { SidebarTitleActionType, getDataProduct, isOutputPort } from '../shared/utils';
import { Preview } from './preview/Preview';
import SidebarNotesSection from '../shared/sidebarSection/SidebarNotesSection';

const headerDropdownItems = new Set([
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.RAISE_INCIDENT,
    EntityMenuItems.ANNOUNCE,
]);

/**
 * Definition of the DataHub MLFeature entity.
 */
export class MLFeatureEntity implements Entity<MlFeature> {
    type: EntityType = EntityType.Mlfeature;

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

    getGraphName = () => 'mlFeature';

    getPathName = () => 'features';

    getEntityName = () => 'Feature';

    getCollectionName = () => 'Features';

    getOverridePropertiesFromEntity = (feature?: MlFeature | null): GenericEntityProperties => {
        return {
            // eslint-disable-next-line
            platform: feature?.['featureTables']?.relationships?.[0]?.entity?.platform,
        };
    };

    useEntityQuery = useGetMlFeatureQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.Mlfeature}
            useEntityQuery={useGetMlFeatureQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
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
                    icon: WarningOutlined,
                    component: IncidentTab,
                    getDynamicName: (_, mlFeature, loading) => {
                        const activeIncidentCount = mlFeature?.mlFeature?.activeIncidents?.total;
                        return <TabNameWithCount name="Incidents" count={activeIncidentCount} loading={loading} />;
                    },
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
            component: SidebarNotesSection,
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

    renderPreview = (previewType: PreviewType, data: MlFeature) => {
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
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
                browsePaths={data.browsePathV2 || undefined}
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
                data={genericProperties}
                name={data.name || ''}
                featureNamespace={data.featureNamespace || ''}
                description={data.description || ''}
                owners={data.ownership?.owners}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                platform={platform}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                degree={(result as any).degree}
                paths={(result as any).paths}
                isOutputPort={isOutputPort(result)}
                headerDropdownItems={headerDropdownItems}
                browsePaths={data.browsePathV2 || undefined}
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
            EntityCapabilityType.LINEAGE,
        ]);
    };
}
