import * as React from 'react';
import { FolderOutlined } from '@ant-design/icons';
import { Container, EntityType, SearchResult } from '../../../types.generated';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { useGetContainerQuery } from '../../../graphql/container.generated';
import { ContainerEntitiesTab } from './ContainerEntitiesTab';
import { SidebarRecommendationsSection } from '../shared/containers/profile/sidebar/Recommendations/SidebarRecommendationsSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';

/**
 * Definition of the DataHub Container entity.
 */
export class ContainerEntity implements Entity<Container> {
    type: EntityType = EntityType.Container;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <FolderOutlined />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <FolderOutlined style={{ fontSize, color: '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
        }

        return (
            <FolderOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'container';

    getEntityName = () => 'Container';

    getCollectionName = () => 'Containers';

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Container}
            useEntityQuery={useGetContainerQuery}
            useUpdateQuery={undefined}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            tabs={[
                {
                    name: 'Entities',
                    component: ContainerEntitiesTab,
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
            sidebarSections={[
                {
                    component: SidebarAboutSection,
                },
                {
                    component: SidebarTagsSection,
                    properties: {
                        hasTags: true,
                        hasTerms: true,
                    },
                },
                {
                    component: SidebarOwnerSection,
                },
                {
                    component: SidebarDomainSection,
                },
                {
                    component: SidebarRecommendationsSection,
                },
            ]}
        />
    );

    renderPreview = (_: PreviewType, data: Container) => {
        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                platformName={data.platform.properties?.displayName || data.platform.name}
                platformLogo={data.platform.properties?.logoUrl}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                subTypes={data.subTypes}
                container={data.container}
                entityCount={data.entities?.total}
                domain={data.domain?.domain}
                tags={data.tags}
                externalUrl={data.properties?.externalUrl}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Container;
        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                platformName={data.platform.properties?.displayName || data.platform.name}
                platformLogo={data.platform.properties?.logoUrl}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                description={data.editableProperties?.description || data.properties?.description}
                owners={data.ownership?.owners}
                subTypes={data.subTypes}
                container={data.container}
                entityCount={data.entities?.total}
                domain={data.domain?.domain}
                parentContainers={data.parentContainers}
                externalUrl={data.properties?.externalUrl}
                tags={data.tags}
                glossaryTerms={data.glossaryTerms}
            />
        );
    };

    displayName = (data: Container) => {
        return data?.properties?.name || data?.properties?.qualifiedName || data?.urn;
    };

    getOverridePropertiesFromEntity = (data: Container) => {
        return {
            name: this.displayName(data),
            entityCount: data.entities?.total,
        };
    };

    getGenericEntityProperties = (data: Container) => {
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
            EntityCapabilityType.SOFT_DELETE,
        ]);
    };
}
