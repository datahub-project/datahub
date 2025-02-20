import * as React from 'react';
import { CodeSandboxOutlined } from '@ant-design/icons';
import { MlModel, EntityType, SearchResult, OwnershipType } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { useGetMlModelQuery } from '../../../graphql/mlModel.generated';
import { GenericEntityProperties } from '../shared/types';
import MLModelSummary from './profile/MLModelSummary';
import MLModelGroupsTab from './profile/MLModelGroupsTab';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import MlModelFeaturesTab from './profile/MlModelFeaturesTab';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityDropdown';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import SidebarStructuredPropsSection from '../shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';
import { IncidentTab } from '../shared/tabs/Incident/IncidentTab';

/**
 * Definition of the DataHub MlModel entity.
 */
export class MLModelEntity implements Entity<MlModel> {
    type: EntityType = EntityType.Mlmodel;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <CodeSandboxOutlined style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <CodeSandboxOutlined style={{ fontSize, color: color || '#9633b9' }} />;
        }

        return (
            <CodeSandboxOutlined
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'mlModel';

    getPathName = () => 'mlModels';

    getEntityName = () => 'ML Model';

    getCollectionName = () => 'ML Models';

    getOverridePropertiesFromEntity = (mlModel?: MlModel | null): GenericEntityProperties => {
        return {
            externalUrl: mlModel?.properties?.externalUrl,
        };
    };

    useEntityQuery = useGetMlModelQuery;

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

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.Mlmodel}
            useEntityQuery={useGetMlModelQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={new Set([EntityMenuItems.UPDATE_DEPRECATION])}
            tabs={[
                {
                    name: 'Summary',
                    component: MLModelSummary,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    name: 'Group',
                    component: MLModelGroupsTab,
                },
                {
                    name: 'Features',
                    component: MlModelFeaturesTab,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
                {
                    name: 'Incidents',
                    component: IncidentTab,
                    getDynamicName: (_, mlModel) => {
                        const activeIncidentCount = mlModel?.mlModel?.activeIncidents?.total;
                        return `Incidents${(activeIncidentCount && ` (${activeIncidentCount})`) || ''}`;
                    },
                },
            ]}
            sidebarSections={this.getSidebarSections()}
        />
    );

    renderPreview = (_: PreviewType, data: MlModel) => {
        return <Preview model={data} />;
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlModel;
        return <Preview model={data} degree={(result as any).degree} paths={(result as any).paths} />;
    };

    getLineageVizConfig = (entity: MlModel) => {
        return {
            urn: entity.urn,
            // eslint-disable-next-line @typescript-eslint/dot-notation
            name: entity.properties?.['propertiesName'] || entity.name,
            type: EntityType.Mlmodel,
            icon: entity.platform?.properties?.logoUrl || undefined,
            platform: entity.platform,
        };
    };

    displayName = (data: MlModel) => {
        return data.properties?.name || data.name || data.urn;
    };

    getGenericEntityProperties = (mlModel: MlModel) => {
        return getDataForEntityType({ data: mlModel, entityType: this.type, getOverrideProperties: (data) => data });
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
