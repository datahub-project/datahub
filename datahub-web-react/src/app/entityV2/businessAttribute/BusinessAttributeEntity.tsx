import { Hexagon } from '@phosphor-icons/react';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/businessAttribute/preview/Preview';
import { BusinessAttributeDataTypeSection } from '@app/entityV2/businessAttribute/profile/BusinessAttributeDataTypeSection';
import BusinessAttributeRelatedEntity from '@app/entityV2/businessAttribute/profile/BusinessAttributeRelatedEntity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { PageRoutes } from '@conf/Global';

import { useGetBusinessAttributeQuery } from '@graphql/businessAttribute.generated';
import { BusinessAttribute, EntityType, SearchResult } from '@types';

const headerDropdownItems = new Set([EntityMenuItems.DELETE]);

/**
 *  Definition of datahub Business Attribute Entity
 */
/* eslint-disable @typescript-eslint/no-unused-vars */
export class BusinessAttributeEntity implements Entity<BusinessAttribute> {
    type: EntityType = EntityType.BusinessAttribute;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
        }

        return (
            <Hexagon
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
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
                data={data}
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
                headerDropdownItems={headerDropdownItems}
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
                            customTagPath: 'properties.tags',
                        },
                    },
                    {
                        component: SidebarGlossaryTermsSection,
                        properties: {
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
            EntityCapabilityType.BUSINESS_ATTRIBUTES,
        ]);
    };
}
