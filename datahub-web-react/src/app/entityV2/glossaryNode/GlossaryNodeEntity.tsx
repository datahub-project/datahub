import { AppstoreOutlined, FileOutlined, UnorderedListOutlined } from '@ant-design/icons';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import i18next from 'i18next';
import React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import ChildrenTabWrapper from '@app/entityV2/glossaryNode/ChildrenTabWrapper';
import { Preview } from '@app/entityV2/glossaryNode/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarApplicationSection } from '@app/entityV2/shared/containers/profile/sidebar/Applications/SidebarApplicationSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import SidebarNotesSection from '@app/entityV2/shared/sidebarSection/SidebarNotesSection';
import SidebarStructuredProperties from '@app/entityV2/shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { EntityTab } from '@app/entityV2/shared/types';
import SummaryTab from '@app/entityV2/summary/SummaryTab';
import { useShowAssetSummaryPage } from '@app/entityV2/summary/useShowAssetSummaryPage';
import { FetchedEntity } from '@app/lineage/types';

import { useGetGlossaryNodeQuery } from '@graphql/glossaryNode.generated';
import { EntityType, GlossaryNode, SearchResult } from '@types';

const headerDropdownItems = new Set([
    EntityMenuItems.MOVE,
    EntityMenuItems.SHARE,
    EntityMenuItems.CLONE,
    EntityMenuItems.DELETE,
    EntityMenuItems.ANNOUNCE,
]);

class GlossaryNodeEntity implements Entity<GlossaryNode> {
    getLineageVizConfig?: ((entity: GlossaryNode) => FetchedEntity) | undefined;

    type: EntityType = EntityType.GlossaryNode;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <BookmarksSimple
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    isLineageEnabled = () => false;

    getPathName = () => 'glossaryNode';

    getCollectionName = () => i18next.t('entity.types:glossaryNode.namePlural');

    getEntityName = () => i18next.t('entity.types:glossaryNode.name');

    useEntityQuery = useGetGlossaryNodeQuery;

    renderProfile = (urn: string) => {
        return (
            <EntityProfile
                urn={urn}
                entityType={EntityType.GlossaryNode}
                useEntityQuery={useGetGlossaryNodeQuery}
                getOverrideProperties={this.getOverridePropertiesFromEntity}
                isNameEditable
                tabs={this.getProfileTabs()}
                sidebarSections={this.getSidebarSections()}
                // NOTE: Hiding this for now as we've moved the actions to the content of ChildrenTab.tsx
                // The buttons are too big and causes other actions to overflow.
                // This component requires deeper refactoring to dynamically adapt to smaller screens.
                // headerActionItems={
                //     new Set([EntityActionItem.ADD_CHILD_GLOSSARY_NODE, EntityActionItem.ADD_CHILD_GLOSSARY_TERM])
                // }
                headerDropdownItems={headerDropdownItems}
                sidebarTabs={this.getSidebarTabs()}
            />
        );
    };

    getSidebarSections = () => [
        {
            component: SidebarAboutSection,
            properties: {
                hideLinksButton: true,
            },
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
                      },
                  ]
                : []),
            {
                name: i18next.t('entity.types:tab.contents'),
                component: ChildrenTabWrapper,
                icon: AppstoreOutlined,
            },
            ...(!showSummaryTab
                ? [
                      {
                          name: i18next.t('entity.types:tab.documentation'),
                          component: DocumentationTab,
                          icon: FileOutlined,
                          properties: {
                              hideLinksButton: true,
                          },
                      },
                  ]
                : []),
            {
                name: i18next.t('entity.types:tab.properties'),
                component: PropertiesTab,
                icon: UnorderedListOutlined,
            },
        ];
    };

    getSidebarTabs = () => [
        {
            name: i18next.t('entity.types:tab.properties'),
            component: PropertiesTab,
            description: i18next.t('entity.types:sidebar.propertiesDescription'),
            icon: UnorderedListOutlined,
        },
    ];

    displayName = (data: GlossaryNode) => {
        return data?.properties?.name || data?.urn;
    };

    getOverridePropertiesFromEntity = (data: GlossaryNode) => {
        return {
            name: this.displayName(data),
        };
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as GlossaryNode);
    };

    renderPreview = (previewType: PreviewType, data: GlossaryNode) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data?.urn}
                data={genericProperties}
                parentNodes={data.parentNodes}
                name={this.displayName(data)}
                description={data?.properties?.description || ''}
                owners={data?.ownership?.owners}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
            />
        );
    };

    platformLogoUrl = (_: GlossaryNode) => {
        return undefined;
    };

    getGenericEntityProperties = (glossaryNode: GlossaryNode) => {
        return getDataForEntityType({
            data: glossaryNode,
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
            EntityCapabilityType.RELATED_DOCUMENTS,
            EntityCapabilityType.FORMS,
        ]);
    };

    getGraphName = () => this.getPathName();
}

export default GlossaryNodeEntity;
