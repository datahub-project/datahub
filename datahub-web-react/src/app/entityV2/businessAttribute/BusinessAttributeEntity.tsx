import * as React from 'react';
import { GlobalOutlined } from '@ant-design/icons';
import { BusinessAttribute, EntityType, SearchResult } from '../../../types.generated';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { useGetBusinessAttributeQuery } from '../../../graphql/businessAttribute.generated';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { Preview } from './preview/Preview';
import { PageRoutes } from '../../../conf/Global';
import BusinessAttributeRelatedEntity from './profile/BusinessAttributeRelatedEntity';
import { BusinessAttributeDataTypeSection } from './profile/BusinessAttributeDataTypeSection';

/**
 *  Definition of datahub Business Attribute Entity
 */
/* eslint-disable @typescript-eslint/no-unused-vars */
export class BusinessAttributeEntity implements Entity<BusinessAttribute> {
    type: EntityType = EntityType.BusinessAttribute;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <GlobalOutlined style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <GlobalOutlined style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            // TODO: Update the returned path value to the correct svg icon path
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
        }

        return (
            <GlobalOutlined
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    displayName = (data: BusinessAttribute) => {
        return data?.properties?.name || data?.urn;
    };

    getPathName = () => 'business-attribute';

    getEntityName = () => 'Business Attribute';

    getCollectionName = () => 'Business Attributes';

    getGraphName = () => 'businessAttribute';

    getCustomCardUrlPath = () => PageRoutes.BUSINESS_ATTRIBUTE;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    isSearchEnabled = () => true;

    getOverridePropertiesFromEntity = (data: BusinessAttribute) => {
        return {
            name: data.properties?.name,
        };
    };

    getGenericEntityProperties = (data: BusinessAttribute) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    renderPreview = (previewType: PreviewType, data: BusinessAttribute) => {
        return (
            <Preview
                previewType={previewType}
                urn={data.urn}
                name={this.displayName(data)}
                description={data.properties?.description || ''}
                owners={data.ownership?.owners}
            />
        );
    };

    renderProfile = (urn: string) => {
        return (
            <EntityProfile
                urn={urn}
                entityType={EntityType.BusinessAttribute}
                useEntityQuery={useGetBusinessAttributeQuery as any}
                headerDropdownItems={new Set([EntityMenuItems.DELETE])}
                isNameEditable
                tabs={[
                    {
                        name: 'Documentation',
                        component: DocumentationTab,
                    },
                    {
                        name: 'Related Entities',
                        component: BusinessAttributeRelatedEntity,
                    },
                    {
                        name: 'Properties',
                        component: PropertiesTab,
                    },
                ]}
                sidebarSections={[
                    {
                        component: SidebarAboutSection,
                    },
                    {
                        component: BusinessAttributeDataTypeSection,
                    },
                    {
                        component: SidebarOwnerSection,
                    },
                    {
                        component: SidebarTagsSection,
                        properties: {
                            hasTags: true,
                            hasTerms: true,
                            customTagPath: 'properties.tags',
                            customTermPath: 'properties.glossaryTerms',
                        },
                    },
                ]}
                getOverrideProperties={this.getOverridePropertiesFromEntity}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as BusinessAttribute);
    };

    supportedCapabilities = () => {
        return new Set([
            EntityCapabilityType.OWNERS,
            EntityCapabilityType.TAGS,
            EntityCapabilityType.GLOSSARY_TERMS,
            // EntityCapabilityType.BUSINESS_ATTRIBUTES,
        ]);
    };
}
