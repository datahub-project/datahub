import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { Columns } from '@phosphor-icons/react/dist/csr/Columns';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { SquaresFour } from '@phosphor-icons/react/dist/csr/SquaresFour';
import i18next from 'i18next';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/glossaryTerm/preview/Preview';
import { RelatedTermTypes } from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTermsResult';
import useGlossaryRelatedAssetsTabCount from '@app/entityV2/glossaryTerm/profile/useGlossaryRelatedAssetsTabCount';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/entityData';
import { EntityActionItem } from '@app/entityV2/shared/entity/EntityActions';
import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import { EntityTab } from '@app/entityV2/shared/types';
import { useShowAssetSummaryPage } from '@app/entityV2/summary/useShowAssetSummaryPage';
import { FetchedEntity } from '@app/lineage/types';

import { GetGlossaryTermQuery, useGetGlossaryTermQuery } from '@graphql/glossaryTerm.generated';
import { EntityType, GlossaryTerm, SearchResult } from '@types';

const GlossaryRelatedEntity = lazyProfileComponent(
    'GlossaryRelatedEntity',
    () => import('@app/entityV2/glossaryTerm/profile/GlossaryRelatedEntity'),
);
const GlossayRelatedTerms = lazyProfileComponent(
    'GlossayRelatedTerms',
    () => import('@app/entityV2/glossaryTerm/profile/GlossaryRelatedTerms'),
);
const EntityProfile = lazyProfileComponent('EntityProfile', () =>
    import('@app/entityV2/shared/containers/profile/EntityProfile').then((module) => ({
        default: module.EntityProfile,
    })),
);
const SidebarAboutSection = lazyProfileComponent('SidebarAboutSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection').then((module) => ({
        default: module.SidebarAboutSection,
    })),
);
const SidebarApplicationSection = lazyProfileComponent('SidebarApplicationSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/Applications/SidebarApplicationSection').then((module) => ({
        default: module.SidebarApplicationSection,
    })),
);
const SidebarDomainSection = lazyProfileComponent('SidebarDomainSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection').then((module) => ({
        default: module.SidebarDomainSection,
    })),
);
const SidebarOwnerSection = lazyProfileComponent('SidebarOwnerSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection').then((module) => ({
        default: module.SidebarOwnerSection,
    })),
);
const SidebarEntityHeader = lazyProfileComponent(
    'SidebarEntityHeader',
    () => import('@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader'),
);
const SidebarTagsSection = lazyProfileComponent('SidebarTagsSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection').then((module) => ({
        default: module.SidebarTagsSection,
    })),
);
const StatusSection = lazyProfileComponent(
    'StatusSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection'),
);
const SidebarNotesSection = lazyProfileComponent(
    'SidebarNotesSection',
    () => import('@app/entityV2/shared/sidebarSection/SidebarNotesSection'),
);
const SidebarStructuredProperties = lazyProfileComponent(
    'SidebarStructuredProperties',
    () => import('@app/entityV2/shared/sidebarSection/SidebarStructuredProperties'),
);
const SchemaTab = lazyProfileComponent('SchemaTab', () =>
    import('@app/entityV2/shared/tabs/Dataset/Schema/SchemaTab').then((module) => ({
        default: module.SchemaTab,
    })),
);
const DocumentationTab = lazyProfileComponent('DocumentationTab', () =>
    import('@app/entityV2/shared/tabs/Documentation/DocumentationTab').then((module) => ({
        default: module.DocumentationTab,
    })),
);
const PropertiesTab = lazyProfileComponent('PropertiesTab', () =>
    import('@app/entityV2/shared/tabs/Properties/PropertiesTab').then((module) => ({
        default: module.PropertiesTab,
    })),
);
const SummaryTab = lazyProfileComponent('SummaryTab', () => import('@app/entityV2/summary/SummaryTab'));

const headerDropdownItems = new Set([
    EntityMenuItems.EDIT_GLOSSARY,
    EntityMenuItems.CHANGE_HISTORY,
    EntityMenuItems.MOVE,
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.CLONE,
    EntityMenuItems.DELETE,
    EntityMenuItems.ANNOUNCE,
]);

/**
 * Definition of the DataHub Dataset entity.
 */
export class GlossaryTermEntity implements Entity<GlossaryTerm> {
    getLineageVizConfig?: ((entity: GlossaryTerm) => FetchedEntity) | undefined;

    type: EntityType = EntityType.GlossaryTerm;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <BookmarkSimple
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    isLineageEnabled = () => false;

    getPathName = () => 'glossaryTerm';

    getCollectionName = () => i18next.t('entity.types:glossaryTerm.namePlural');

    getEntityName = () => i18next.t('entity.types:glossaryTerm.name');

    useEntityQuery = useGetGlossaryTermQuery;

    renderProfile = (urn) => {
        return (
            <EntityProfile
                urn={urn}
                entityType={EntityType.GlossaryTerm}
                useEntityQuery={useGetGlossaryTermQuery as any}
                headerActionItems={new Set([EntityActionItem.BATCH_ADD_GLOSSARY_TERM])}
                headerDropdownItems={headerDropdownItems}
                tabs={this.getProfileTabs()}
                sidebarSections={this.getSidebarSections()}
                getOverrideProperties={this.getOverridePropertiesFromEntity}
                sidebarTabs={this.getSidebarTabs()}
            />
        );
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
            component: SidebarOwnerSection,
        },
        {
            component: SidebarDomainSection,
            properties: {
                hideOwnerType: true,
            },
        },
        {
            component: SidebarApplicationSection,
        },
        {
            component: SidebarTagsSection,
        },
        {
            component: SidebarStructuredProperties,
        },
        {
            component: StatusSection,
        },
    ];

    getProfileTabs = (): EntityTab[] => {
        const showSummaryTab = useShowAssetSummaryPage();

        return [
            ...(showSummaryTab
                ? [
                      {
                          name: i18next.t('entity.types:tab.summary'),
                          component: SummaryTab,
                          id: 'asset-summary-tab',
                      },
                  ]
                : []),
            ...(!showSummaryTab
                ? [
                      {
                          name: i18next.t('entity.types:tab.documentation'),
                          component: DocumentationTab,
                          icon: FileText,
                      },
                  ]
                : []),
            {
                name: i18next.t('entity.types:shared.relatedAssets'),
                getCount: useGlossaryRelatedAssetsTabCount,
                component: GlossaryRelatedEntity,
                icon: SquaresFour,
            },
            {
                name: i18next.t('entity.types:glossaryTerm.schemaTab'),
                component: SchemaTab,
                icon: Columns,
                properties: {
                    editMode: false,
                },
                display: {
                    visible: (_, glossaryTerm: GetGlossaryTermQuery) =>
                        glossaryTerm?.glossaryTerm?.schemaMetadata !== null,
                    enabled: (_, glossaryTerm: GetGlossaryTermQuery) =>
                        glossaryTerm?.glossaryTerm?.schemaMetadata !== null,
                },
            },
            {
                name: i18next.t('entity.types:glossaryTerm.relatedTermsTab'),
                getCount: (entityData, _, loading) => {
                    const totalRelatedTerms = Object.keys(RelatedTermTypes).reduce((acc, curr) => {
                        return acc + (entityData?.[curr]?.total || 0);
                    }, 0);
                    return !loading ? totalRelatedTerms : undefined;
                },
                component: GlossayRelatedTerms,
                icon: () => <BookmarkSimple style={{ marginRight: 6 }} />,
            },
            {
                name: i18next.t('entity.types:tab.properties'),
                component: PropertiesTab,
                icon: ListBullets,
            },
        ];
    };

    getSidebarTabs = () => [
        {
            name: i18next.t('entity.types:tab.properties'),
            component: PropertiesTab,
            description: i18next.t('entity.types:sidebar.propertiesDescription'),
            icon: ListBullets,
        },
    ];

    getOverridePropertiesFromEntity = (glossaryTerm?: GlossaryTerm | null): GenericEntityProperties => {
        // if dataset has subTypes filled out, pick the most specific subtype and return it
        return {
            customProperties: glossaryTerm?.properties?.customProperties,
        };
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as GlossaryTerm);
    };

    renderPreview = (previewType: PreviewType, data: GlossaryTerm) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                data={genericProperties}
                previewType={previewType}
                urn={data?.urn}
                parentNodes={data.parentNodes}
                name={this.displayName(data)}
                description={data?.properties?.description || ''}
                owners={data?.ownership?.owners}
                deprecation={data?.deprecation}
                domain={data.domain?.domain}
                headerDropdownItems={headerDropdownItems}
            />
        );
    };

    displayName = (data: GlossaryTerm) => {
        return data?.properties?.name || data?.name || data?.urn;
    };

    platformLogoUrl = (_: GlossaryTerm) => {
        return undefined;
    };

    getGenericEntityProperties = (glossaryTerm: GlossaryTerm) => {
        return getDataForEntityType({
            data: glossaryTerm,
            entityType: this.type,
            getOverrideProperties: (data) => data,
        });
    };

    supportedCapabilities = () => {
        return new Set([
            EntityCapabilityType.OWNERS,
            EntityCapabilityType.DEPRECATION,
            EntityCapabilityType.SOFT_DELETE,
            EntityCapabilityType.APPLICATIONS,
            EntityCapabilityType.DOMAINS,
            EntityCapabilityType.TEST,
            EntityCapabilityType.RELATED_DOCUMENTS,
            EntityCapabilityType.FORMS,
            EntityCapabilityType.TAGS,
        ]);
    };

    getGraphName = () => this.getPathName();
}
