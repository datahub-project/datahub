import { AppstoreOutlined, FileOutlined, UnorderedListOutlined } from '@ant-design/icons';
import React from 'react';
import { BookmarksSimple } from '@phosphor-icons/react';
import { useGetGlossaryNodeQuery } from '../../../graphql/glossaryNode.generated';
import { EntityType, GlossaryNode, SearchResult } from '../../../types.generated';
import { FetchedEntity } from '../../lineage/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import StatusSection from '../shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityActionItem } from '../shared/entity/EntityActions';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import SidebarStructuredProperties from '../shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import ChildrenTab from './ChildrenTab';
import { Preview } from './preview/Preview';
import SidebarNotesSection from '../shared/sidebarSection/SidebarNotesSection';

const headerDropdownItems = new Set([
    EntityMenuItems.MOVE,
    EntityMenuItems.SHARE,
    EntityMenuItems.DELETE,
    EntityMenuItems.ANNOUNCE,
]);

class GlossaryNodeEntity implements Entity<GlossaryNode> {
    getLineageVizConfig?: ((entity: GlossaryNode) => FetchedEntity) | undefined;

    type: EntityType = EntityType.GlossaryNode;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <BookmarksSimple className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <BookmarksSimple
                    className={TYPE_ICON_CLASS_NAME}
                    style={{ fontSize, color: color || '#B37FEB' }}
                    weight="fill"
                />
            );
        }

        if (styleType === IconStyleType.ACCENT) {
            return <BookmarksSimple style={{ fontSize: fontSize || 10, color: color || '#6C6B88' }} />;
        }

        return (
            <BookmarksSimple
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

    getAutoCompleteFieldName = () => 'name';

    isLineageEnabled = () => false;

    getPathName = () => 'glossaryNode';

    getCollectionName = () => 'Term Groups';

    getEntityName = () => 'Term Group';

    useEntityQuery = useGetGlossaryNodeQuery;

    renderProfile = (urn: string) => {
        return (
            <EntityProfile
                urn={urn}
                entityType={EntityType.GlossaryNode}
                useEntityQuery={useGetGlossaryNodeQuery}
                getOverrideProperties={this.getOverridePropertiesFromEntity}
                isNameEditable
                tabs={[
                    {
                        name: 'Contents',
                        component: ChildrenTab,
                        icon: AppstoreOutlined,
                    },
                    {
                        name: 'Documentation',
                        component: DocumentationTab,
                        icon: FileOutlined,
                        properties: {
                            hideLinksButton: true,
                        },
                    },
                    {
                        name: 'Properties',
                        component: PropertiesTab,
                        icon: UnorderedListOutlined,
                    },
                ]}
                sidebarSections={this.getSidebarSections()}
                headerActionItems={
                    new Set([EntityActionItem.ADD_CHILD_GLOSSARY_NODE, EntityActionItem.ADD_CHILD_GLOSSARY_TERM])
                }
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
            component: SidebarStructuredProperties,
        },
        {
            component: StatusSection,
        },
    ];

    getSidebarTabs = () => [
        {
            name: 'Properties',
            component: PropertiesTab,
            description: 'View additional properties about this asset',
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
        ]);
    };

    getGraphName = () => this.getPathName();
}

export default GlossaryNodeEntity;
