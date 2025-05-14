import { FolderFilled, FolderOutlined } from '@ant-design/icons';
import React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entity/Entity';
import ChildrenTab from '@app/entity/glossaryNode/ChildrenTab';
import { Preview } from '@app/entity/glossaryNode/preview/Preview';
import { EntityMenuItems } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { EntityProfile } from '@app/entity/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entity/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarOwnerSection } from '@app/entity/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarStructuredPropsSection from '@app/entity/shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';
import { getDataForEntityType } from '@app/entity/shared/containers/profile/utils';
import { DocumentationTab } from '@app/entity/shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '@app/entity/shared/tabs/Properties/PropertiesTab';

import { useGetGlossaryNodeQuery } from '@graphql/glossaryNode.generated';
import { EntityType, GlossaryNode, SearchResult } from '@types';

class GlossaryNodeEntity implements Entity<GlossaryNode> {
    type: EntityType = EntityType.GlossaryNode;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <FolderOutlined style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <FolderFilled style={{ fontSize, color: color || '#B37FEB' }} />;
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
                hideBrowseBar
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
                    {
                        name: 'Properties',
                        component: PropertiesTab,
                    },
                ]}
                sidebarSections={this.getSidebarSections()}
                headerDropdownItems={
                    new Set([
                        EntityMenuItems.ADD_TERM_GROUP,
                        EntityMenuItems.ADD_TERM,
                        EntityMenuItems.MOVE,
                        EntityMenuItems.DELETE,
                    ])
                }
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
            component: SidebarOwnerSection,
        },
        {
            component: SidebarStructuredPropsSection,
        },
    ];

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

    getGraphName = () => this.getPathName();
}

export default GlossaryNodeEntity;
