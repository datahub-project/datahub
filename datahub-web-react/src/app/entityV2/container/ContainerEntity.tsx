import { AppstoreOutlined, FileOutlined, UnlockOutlined } from '@ant-design/icons';
import { Folder, ListBullets } from '@phosphor-icons/react';
import * as React from 'react';

import AccessManagement from '@app/entity/shared/tabs/Dataset/AccessManagement/AccessManagement';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { ContainerEntitiesTab } from '@app/entityV2/container/ContainerEntitiesTab';
import ContainerSummaryTab from '@app/entityV2/container/ContainerSummaryTab';
import { Preview } from '@app/entityV2/container/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { SubType, TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import SidebarContentsSection from '@app/entityV2/shared/containers/profile/sidebar/Container/SidebarContentsSection';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import EmbeddedProfile from '@app/entityV2/shared/embed/EmbeddedProfile';
import SidebarNotesSection from '@app/entityV2/shared/sidebarSection/SidebarNotesSection';
import SidebarStructuredProperties from '@app/entityV2/shared/sidebarSection/SidebarStructuredProperties';
import { SUMMARY_TAB_ICON } from '@app/entityV2/shared/summary/HeaderComponents';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { getDataProduct, getFirstSubType, isOutputPort } from '@app/entityV2/shared/utils';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useAppConfig } from '@app/useAppConfig';

import { GetContainerQuery, useGetContainerQuery } from '@graphql/container.generated';
import { Container, EntityType, SearchResult } from '@types';

const headerDropdownItems = new Set([EntityMenuItems.SHARE, EntityMenuItems.ANNOUNCE]);

/**
 * Definition of the DataHub Container entity.
 */
export class ContainerEntity implements Entity<Container> {
    type: EntityType = EntityType.Container;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
        }

        return (
            <Folder
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
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

    appconfig = useAppConfig;

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
                    icon: ListBullets,
                },
                {
                    name: 'Access',
                    component: AccessManagement,
                    icon: UnlockOutlined,
                    display: {
                        visible: (_, container: GetContainerQuery) => {
                            return (
                                this.appconfig().config.featureFlags.showAccessManagement &&
                                !!container?.container?.access
                            );
                        },
                        enabled: (_, _2) => true,
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
            icon: ListBullets,
        },
    ];

    renderPreview = (previewType: PreviewType, data: Container) => {
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
                previewType={previewType}
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
                previewType={PreviewType.SEARCH}
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
            subtype: getFirstSubType(entity) || undefined,
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
            EntityCapabilityType.RELATED_DOCUMENTS,
            EntityCapabilityType.FORMS,
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
