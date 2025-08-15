import { BookFilled, BookOutlined } from '@ant-design/icons';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entity/Entity';
import { Preview } from '@app/entity/glossaryTerm/preview/Preview';
import GlossaryRelatedEntity from '@app/entity/glossaryTerm/profile/GlossaryRelatedEntity';
import GlossayRelatedTerms from '@app/entity/glossaryTerm/profile/GlossaryRelatedTerms';
import { EntityMenuItems } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { EntityProfile } from '@app/entity/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entity/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarDomainSection } from '@app/entity/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entity/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarStructuredPropsSection from '@app/entity/shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';
import { getDataForEntityType } from '@app/entity/shared/containers/profile/utils';
import { EntityActionItem } from '@app/entity/shared/entity/EntityActions';
import { SchemaTab } from '@app/entity/shared/tabs/Dataset/Schema/SchemaTab';
import { DocumentationTab } from '@app/entity/shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '@app/entity/shared/tabs/Properties/PropertiesTab';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { PageRoutes } from '@conf/Global';

import { GetGlossaryTermQuery, useGetGlossaryTermQuery } from '@graphql/glossaryTerm.generated';
import { EntityType, GlossaryTerm, SearchResult } from '@types';

/**
 * Definition of the DataHub Dataset entity.
 */
export class GlossaryTermEntity implements Entity<GlossaryTerm> {
    type: EntityType = EntityType.GlossaryTerm;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <BookOutlined style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <BookFilled style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        return (
            <BookOutlined
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    isLineageEnabled = () => false;

    getPathName = () => 'glossaryTerm';

    getCollectionName = () => 'Glossary Terms';

    getEntityName = () => 'Glossary Term';

    useEntityQuery = useGetGlossaryTermQuery;

    getCustomCardUrlPath = () => PageRoutes.GLOSSARY;

    renderProfile = (urn) => {
        return (
            <EntityProfile
                urn={urn}
                entityType={EntityType.GlossaryTerm}
                useEntityQuery={useGetGlossaryTermQuery as any}
                headerActionItems={new Set([EntityActionItem.BATCH_ADD_GLOSSARY_TERM])}
                headerDropdownItems={
                    new Set([
                        EntityMenuItems.UPDATE_DEPRECATION,
                        EntityMenuItems.CLONE,
                        EntityMenuItems.MOVE,
                        EntityMenuItems.DELETE,
                    ])
                }
                isNameEditable
                hideBrowseBar
                tabs={[
                    {
                        name: 'Documentation',
                        component: DocumentationTab,
                    },
                    {
                        name: 'Related Entities',
                        component: GlossaryRelatedEntity,
                    },
                    {
                        name: 'Schema',
                        component: SchemaTab,
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
                        name: 'Related Terms',
                        component: GlossayRelatedTerms,
                    },
                    {
                        name: 'Properties',
                        component: PropertiesTab,
                    },
                ]}
                sidebarSections={this.getSidebarSections()}
                getOverrideProperties={this.getOverridePropertiesFromEntity}
            />
        );
    };

    getSidebarSections = () => [
        {
            component: SidebarAboutSection,
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
            component: SidebarStructuredPropsSection,
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
        return (
            <Preview
                previewType={previewType}
                urn={data?.urn}
                parentNodes={data.parentNodes}
                name={this.displayName(data)}
                description={data?.properties?.description || ''}
                owners={data?.ownership?.owners}
                domain={data.domain?.domain}
            />
        );
    };

    displayName = (data: GlossaryTerm) => {
        return data.properties?.name || data.name || data.urn;
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
        ]);
    };

    getGraphName = () => this.getPathName();
}
