import * as React from 'react';
import { CodeSandboxOutlined } from '@ant-design/icons';
import { MlModelGroup, EntityType, SearchResult, OwnershipType } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { GenericEntityProperties } from '../shared/types';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { useGetMlModelGroupQuery } from '../../../graphql/mlModelGroup.generated';
import ModelGroupModels from './profile/ModelGroupModels';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityDropdown';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import SidebarStructuredPropsSection from '../shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';

/**
 * Definition of the DataHub MlModelGroup entity.
 */
export class MLModelGroupEntity implements Entity<MlModelGroup> {
    type: EntityType = EntityType.MlmodelGroup;

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

    getGraphName = () => 'mlModelGroup';

    getPathName = () => 'mlModelGroup';

    getEntityName = () => 'ML Group';

    getCollectionName = () => 'ML Groups';

    getOverridePropertiesFromEntity = (_?: MlModelGroup | null): GenericEntityProperties => {
        return {};
    };

    useEntityQuery = useGetMlModelGroupQuery;

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
            entityType={EntityType.MlmodelGroup}
            useEntityQuery={useGetMlModelGroupQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={new Set([EntityMenuItems.UPDATE_DEPRECATION])}
            tabs={[
                {
                    name: 'Models',
                    component: ModelGroupModels,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
            ]}
            sidebarSections={this.getSidebarSections()}
        />
    );

    renderPreview = (_: PreviewType, data: MlModelGroup) => {
        return <Preview group={data} />;
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlModelGroup;
        return <Preview group={data} degree={(result as any).degree} paths={(result as any).paths} />;
    };

    getLineageVizConfig = (entity: MlModelGroup) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.MlmodelGroup,
            icon: entity.platform?.properties?.logoUrl || undefined,
            platform: entity.platform,
        };
    };

    displayName = (data: MlModelGroup) => {
        return data.properties?.name || data.name || data.urn;
    };

    getGenericEntityProperties = (mlModelGroup: MlModelGroup) => {
        return getDataForEntityType({
            data: mlModelGroup,
            entityType: this.type,
            getOverrideProperties: (data) => data,
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
}
