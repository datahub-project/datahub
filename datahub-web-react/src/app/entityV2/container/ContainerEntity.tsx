import { AppstoreOutlined, FileOutlined, FolderOutlined, UnorderedListOutlined } from '@ant-design/icons';
import * as React from 'react';
import { GetContainerQuery, useGetContainerQuery } from '../../../graphql/container.generated';
import { Container, EntityType, SearchResult } from '../../../types.generated';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import { SubType, TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import SidebarContentsSection from '../shared/containers/profile/sidebar/Container/SidebarContentsSection';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '../shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '../shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '../shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import EmbeddedProfile from '../shared/embed/EmbeddedProfile';
import SidebarStructuredProperties from '../shared/sidebarSection/SidebarStructuredProperties';
import { SUMMARY_TAB_ICON } from '../shared/summary/HeaderComponents';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { getDataProduct, isOutputPort } from '../shared/utils';
import { ContainerEntitiesTab } from './ContainerEntitiesTab';
import ContainerSummaryTab from './ContainerSummaryTab';
import { Preview } from './preview/Preview';
import SidebarNotesSection from '../shared/sidebarSection/SidebarNotesSection';

const headerDropdownItems = new Set([EntityMenuItems.EXTERNAL_URL, EntityMenuItems.SHARE, EntityMenuItems.ANNOUNCE]);

/**
 * Definition of the DataHub Container entity.
 */
export class ContainerEntity implements Entity<Container> {
    type: EntityType = EntityType.Container;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <FolderOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <FolderOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
        }

        return (
            <FolderOutlined
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

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'container';

    getPathName = () => this.getGraphName();

    getEntityName = () => 'Container';

    getCollectionName = () => 'Containers';

    useEntityQuery = useGetContainerQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Container}
            useEntityQuery={useGetContainerQuery}
            useUpdateQuery={undefined}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            tabs={[
                {
                    name: 'Summary',
                    component: ContainerSummaryTab,
                    icon: SUMMARY_TAB_ICON,
                    display: {
                        visible: (_, container: GetContainerQuery) =>
                            !!container?.container?.subTypes?.typeNames?.includes(SubType.TableauWorkbook),
                        enabled: () => true,
                    },
                },
                {
                    name: 'Contents',
                    component: ContainerEntitiesTab,
                    icon: AppstoreOutlined,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                    icon: FileOutlined,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                    icon: UnorderedListOutlined,
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
            component: SidebarContentsSection,
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
        // TODO: Add back once entity-level recommendations are complete.
        // {
        //    component: SidebarRecommendationsSection,
        // },
    ];

    getSidebarTabs = () => [
        {
            name: 'Properties',
            component: PropertiesTab,
            description: 'View additional properties about this asset',
            icon: UnorderedListOutlined,
        },
    ];

    renderPreview = (_: PreviewType, data: Container) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                platformName={data.platform.properties?.displayName || capitalizeFirstLetterOnly(data.platform.name)}
                platformLogo={data.platform.properties?.logoUrl}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                subTypes={data.subTypes}
                container={data}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                tags={data.tags}
                externalUrl={data.properties?.externalUrl}
                entityCount={data.entities?.total}
                headerDropdownItems={headerDropdownItems}
                browsePaths={data.browsePathV2 || undefined}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Container;
        const genericProperties = this.getGenericEntityProperties(data);

        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                platformName={data.platform.properties?.displayName || capitalizeFirstLetterOnly(data.platform.name)}
                platformLogo={data.platform.properties?.logoUrl}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                description={data.editableProperties?.description || data.properties?.description}
                owners={data.ownership?.owners}
                subTypes={data.subTypes}
                container={data}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                parentContainers={data.parentContainers}
                externalUrl={data.properties?.externalUrl}
                tags={data.tags}
                glossaryTerms={data.glossaryTerms}
                degree={(result as any).degree}
                paths={(result as any).paths}
                entityCount={data.entities?.total}
                isOutputPort={isOutputPort(result)}
                headerDropdownItems={headerDropdownItems}
                browsePaths={data.browsePathV2 || undefined}
            />
        );
    };

    getLineageVizConfig(entity: Container) {
        return {
            urn: entity.urn,
            name: this.displayName(entity),
            type: this.type,
            icon: entity?.platform?.properties?.logoUrl || undefined,
            platform: entity?.platform,
            subtype: entity?.subTypes?.typeNames?.[0] || undefined,
        };
    }

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
            EntityCapabilityType.TEST,
        ]);
    };

    renderEmbeddedProfile = (urn: string) => (
        <EmbeddedProfile
            urn={urn}
            entityType={EntityType.Container}
            useEntityQuery={useGetContainerQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
        />
    );
}
