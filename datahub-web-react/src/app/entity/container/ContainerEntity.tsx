import * as React from 'react';
import { FolderOutlined } from '@ant-design/icons';
import { Container, EntityType, SearchResult } from '../../../types.generated';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { useGetContainerQuery, GetContainerQuery } from '../../../graphql/container.generated';
import { ContainerEntitiesTab } from './ContainerEntitiesTab';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { getDataProduct } from '../shared/utils';
import EmbeddedProfile from '../shared/embed/EmbeddedProfile';
import AccessManagement from '../shared/tabs/Dataset/AccessManagement/AccessManagement';
import { useAppConfig } from '../../useAppConfig';
import SidebarStructuredPropsSection from '../shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';

/**
 * Definition of the DataHub Container entity.
 */
export class ContainerEntity implements Entity<Container> {
    type: EntityType = EntityType.Container;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <FolderOutlined style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <FolderOutlined style={{ fontSize, color: color || '#B37FEB' }} />;
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
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'container';

    getPathName = () => 'container';

    getEntityName = () => 'Container';

    getCollectionName = () => 'Containers';

    useEntityQuery = useGetContainerQuery;

    appconfig = useAppConfig;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Container}
            useEntityQuery={this.useEntityQuery}
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
                {
                    name: 'Access Management',
                    component: AccessManagement,
                    display: {
                        visible: (_, container: GetContainerQuery) => {
                            return (
                                this.appconfig().config.featureFlags.showAccessManagement &&
                                !!container?.container?.access
                            );
                        },
                        enabled: (_, container: GetContainerQuery) => {
                            const accessAspect = container?.container?.access;
                            const rolesList = accessAspect?.roles;
                            return !!accessAspect && !!rolesList && rolesList.length > 0;
                        },
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

    renderPreview = (_: PreviewType, data: Container) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                platformName={data.platform.properties?.displayName || capitalizeFirstLetterOnly(data.platform.name)}
                platformLogo={data.platform.properties?.logoUrl}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                subTypes={data.subTypes}
                container={data.container}
                entityCount={data.entities?.total}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                tags={data.tags}
                externalUrl={data.properties?.externalUrl}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Container;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                platformName={data.platform.properties?.displayName || capitalizeFirstLetterOnly(data.platform.name)}
                platformLogo={data.platform.properties?.logoUrl}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                description={data.editableProperties?.description || data.properties?.description}
                owners={data.ownership?.owners}
                subTypes={data.subTypes}
                container={data.container}
                entityCount={data.entities?.total}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                parentContainers={data.parentContainers}
                externalUrl={data.properties?.externalUrl}
                tags={data.tags}
                glossaryTerms={data.glossaryTerms}
                degree={(result as any).degree}
                paths={(result as any).paths}
            />
        );
    };

    displayName = (data: Container) => {
        return data?.properties?.name || data?.properties?.qualifiedName || data?.urn;
    };

    getOverridePropertiesFromEntity = (data: Container) => {
        return {
            name: this.displayName(data),
            externalUrl: data.properties?.externalUrl,
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
            EntityCapabilityType.DATA_PRODUCTS,
        ]);
    };

    renderEmbeddedProfile = (urn: string) => (
        <EmbeddedProfile
            urn={urn}
            entityType={EntityType.Container}
            useEntityQuery={this.useEntityQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
        />
    );
}
