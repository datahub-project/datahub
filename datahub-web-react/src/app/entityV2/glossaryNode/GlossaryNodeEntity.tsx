import Icon, {
    AppstoreOutlined,
    FileOutlined,
    FolderFilled,
    FolderOutlined,
    UnorderedListOutlined,
} from '@ant-design/icons';
import React from 'react';
import { useGetGlossaryNodeQuery } from '../../../graphql/glossaryNode.generated';
import { EntityType, GlossaryNode, SearchResult } from '../../../types.generated';
import { FetchedEntity } from '../../lineage/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import ChildrenTab from './ChildrenTab';
import { Preview } from './preview/Preview';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { EntityActionItem } from '../shared/entity/EntityActions';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import GlossaryNodeIcon from '../../../images/glossary_collections_bookmark.svg?react';
import StatusSection from '../shared/containers/profile/sidebar/shared/StatusSection';
import SharingAssetSection from '../shared/containers/profile/sidebar/shared/SharingAssetSection';

const headerDropdownItems = new Set([
    EntityMenuItems.MOVE,
    EntityMenuItems.SUBSCRIBE,
    EntityMenuItems.SHARE,
    EntityMenuItems.DELETE,
]);

class GlossaryNodeEntity implements Entity<GlossaryNode> {
    getLineageVizConfig?: ((entity: GlossaryNode) => FetchedEntity) | undefined;

    type: EntityType = EntityType.GlossaryNode;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <FolderOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <FolderFilled className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.ACCENT) {
            return <Icon style={{ fontSize: 10, color: '#6C6B88' }} component={GlossaryNodeIcon} />;
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
            component: StatusSection,
        },
        {
            component: SharingAssetSection,
        },
        {
            component: SidebarOwnerSection,
        },
        {
            component: SidebarAboutSection,
            properties: {
                hideLinksButton: true,
            },
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
        return (
            <Preview
                urn={data?.urn}
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
