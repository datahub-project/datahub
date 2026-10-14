import { File } from '@phosphor-icons/react/dist/csr/File';
import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { LockOpen } from '@phosphor-icons/react/dist/csr/LockOpen';
import { SquaresFour } from '@phosphor-icons/react/dist/csr/SquaresFour';
import i18next from 'i18next';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/container/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { SubType, TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/entityData';
import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import {
    AccessManagement,
    DataProductSection,
    DocumentationTab,
    EmbeddedProfile,
    EntityProfile,
    PropertiesTab,
    SidebarAboutSection,
    SidebarDomainSection,
    SidebarEntityHeader,
    SidebarGlossaryTermsSection,
    SidebarNotesSection,
    SidebarOwnerSection,
    SidebarStructuredProperties,
    SidebarTagsSection,
    StatusSection,
    SummaryTab,
} from '@app/entityV2/shared/profileChunks';
import { SUMMARY_TAB_ICON } from '@app/entityV2/shared/summary/HeaderComponents';
import { EntityTab } from '@app/entityV2/shared/types';
import { getDataProduct, getFirstSubType, isOutputPort } from '@app/entityV2/shared/utils';
import { useShowAssetSummaryPage } from '@app/entityV2/summary/useShowAssetSummaryPage';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useAppConfig } from '@app/useAppConfig';

import { GetContainerQuery, useGetContainerQuery } from '@graphql/container.generated';
import { Container, EntityType, SearchResult } from '@types';

const ContainerSummaryTab = lazyProfileComponent(
    'ContainerSummaryTab',
    () => import('@app/entityV2/container/ContainerSummaryTab'),
);

const ContainerEntitiesTab = lazyProfileComponent('ContainerEntitiesTab', () =>
    import('@app/entityV2/container/ContainerEntitiesTab').then((module) => ({
        default: module.ContainerEntitiesTab,
    })),
);
const SidebarContentsSection = lazyProfileComponent(
    'SidebarContentsSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/Container/SidebarContentsSection'),
);

const headerDropdownItems = new Set([
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.ANNOUNCE,
]);

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

    getEntityName = () => i18next.t('entity.types:container.name');

    getCollectionName = () => i18next.t('entity.types:container.namePlural');

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
            tabs={this.getProfileTabs()}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    getProfileTabs = (): EntityTab[] => {
        const showSummaryTab = useShowAssetSummaryPage();

        return [
            {
                name: i18next.t('entity.types:tab.summary'),
                component: showSummaryTab ? SummaryTab : ContainerSummaryTab,
                icon: SUMMARY_TAB_ICON,
                display: showSummaryTab
                    ? undefined
                    : {
                          visible: (_, container: GetContainerQuery) =>
                              !!container?.container?.subTypes?.typeNames?.includes(SubType.TableauWorkbook),
                          enabled: () => true,
                      },
            },
            {
                name: i18next.t('entity.types:tab.contents'),
                component: ContainerEntitiesTab,
                icon: SquaresFour,
            },
            ...(!showSummaryTab
                ? [
                      {
                          name: i18next.t('entity.types:tab.documentation'),
                          component: DocumentationTab,
                          icon: File,
                      },
                  ]
                : []),
            {
                name: i18next.t('entity.types:tab.properties'),
                component: PropertiesTab,
                icon: ListBullets,
            },
            {
                name: i18next.t('entity.types:shared.accessTab'),
                component: AccessManagement,
                icon: LockOpen,
                display: {
                    visible: (_, container: GetContainerQuery) => {
                        return (
                            this.appconfig().config.featureFlags.showAccessManagement && !!container?.container?.access
                        );
                    },
                    enabled: (_, _2) => true,
                },
            },
        ];
    };

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
            name: i18next.t('entity.types:tab.properties'),
            component: PropertiesTab,
            description: i18next.t('entity.types:sidebar.propertiesDescription'),
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
                deprecation={data.deprecation}
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
                deprecation={data.deprecation}
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
            EntityCapabilityType.DEPRECATION,
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

    getPlatformProperties = (data: Container) => {
        return data?.platform;
    };
}
