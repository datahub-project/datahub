import { FolderFilled, FolderOutlined } from '@ant-design/icons';
import React from 'react';
import { useGetGlossaryNodeQuery } from '../../../graphql/glossaryNode.generated';
import { EntityType, GlossaryNode, SearchResult } from '../../../types.generated';
import GlossaryEntitiesPath from '../../glossary/GlossaryEntitiesPath';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityDropdown';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import ChildrenTab from './ChildrenTab';
import { Preview } from './preview/Preview';

class GlossaryNodeEntity implements Entity<GlossaryNode> {
    type: EntityType = EntityType.GlossaryNode;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <FolderOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <FolderFilled style={{ fontSize, color: '#B37FEB' }} />;
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

    getAutoCompleteFieldName = () => 'name';

    isLineageEnabled = () => false;

    getPathName = () => 'glossaryNode';

    getCollectionName = () => 'Term Groups';

    getEntityName = () => 'Term Group';

    renderProfile = (urn: string) => {
        return (
            <EntityProfile
                urn={urn}
                entityType={EntityType.GlossaryNode}
                useEntityQuery={useGetGlossaryNodeQuery}
                getOverrideProperties={this.getOverridePropertiesFromEntity}
                displayGlossaryBrowser
                isNameEditable
                tabs={[
                    {
                        name: 'Contents',
                        component: ChildrenTab,
                    },
                    {
                        name: 'Documentation',
                        component: DocumentationTab,
                        properties: {
                            hideLinksButton: true,
                        },
                    },
                ]}
                sidebarSections={[
                    {
                        component: SidebarAboutSection,
                        properties: {
                            hideLinksButton: true,
                        },
                    },
                    {
                        component: SidebarOwnerSection,
                        properties: {
                            hideOwnerType: true,
                        },
                    },
                ]}
                customNavBar={<GlossaryEntitiesPath />}
                headerDropdownItems={
                    new Set([
                        EntityMenuItems.COPY_URL,
                        EntityMenuItems.ADD_TERM_GROUP,
                        EntityMenuItems.ADD_TERM,
                        EntityMenuItems.MOVE,
                        EntityMenuItems.DELETE,
                    ])
                }
            />
        );
    };

    displayName = (data: GlossaryNode) => {
        return data.properties?.name || data.urn;
    };

    getOverridePropertiesFromEntity = (data: GlossaryNode) => {
        return {
            name: this.displayName(data),
        };
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as GlossaryNode);
    };

    renderPreview = (_: PreviewType, data: GlossaryNode) => {
        console.log(data);
        return (
            <Preview
                urn={data?.urn}
                parentNodes={data.parentNodes}
                name={this.displayName(data)}
                description={data?.properties?.description || ''}
                owners={data?.ownership?.owners}
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
}

export default GlossaryNodeEntity;
