import { CodeSandboxOutlined } from '@ant-design/icons';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entity/Entity';
import { Preview } from '@app/entity/mlModelGroup/preview/Preview';
import ModelGroupModels from '@app/entity/mlModelGroup/profile/ModelGroupModels';
import { EntityMenuItems } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { EntityProfile } from '@app/entity/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entity/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import DataProductSection from '@app/entity/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entity/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entity/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarTagsSection } from '@app/entity/shared/containers/profile/sidebar/SidebarTagsSection';
import SidebarStructuredPropsSection from '@app/entity/shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';
import { getDataForEntityType } from '@app/entity/shared/containers/profile/utils';
import { DocumentationTab } from '@app/entity/shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '@app/entity/shared/tabs/Properties/PropertiesTab';
import { GenericEntityProperties } from '@app/entity/shared/types';

import { useGetMlModelGroupQuery } from '@graphql/mlModelGroup.generated';
import { EntityType, MlModelGroup, OwnershipType, SearchResult } from '@types';

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
            // eslint-disable-next-line @typescript-eslint/dot-notation
            name: entity.properties?.['propertiesName'] || entity.name,
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
