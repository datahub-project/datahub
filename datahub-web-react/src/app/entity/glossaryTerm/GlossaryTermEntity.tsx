import * as React from 'react';
import { BookFilled, BookOutlined } from '@ant-design/icons';
import { EntityType, GlossaryTerm, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { GetGlossaryTermQuery, useGetGlossaryTermQuery } from '../../../graphql/glossaryTerm.generated';
import { GenericEntityProperties } from '../shared/types';
import { SchemaTab } from '../shared/tabs/Dataset/Schema/SchemaTab';
import GlossaryRelatedEntity from './profile/GlossaryRelatedEntity';
import GlossayRelatedTerms from './profile/GlossaryRelatedTerms';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';

/**
 * Definition of the DataHub Dataset entity.
 */
export class GlossaryTermEntity implements Entity<GlossaryTerm> {
    type: EntityType = EntityType.GlossaryTerm;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <BookOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <BookFilled style={{ fontSize, color: '#B37FEB' }} />;
        }

        return (
            <BookOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    isLineageEnabled = () => false;

    getPathName = () => 'glossary';

    getCollectionName = () => 'Glossary Terms';

    getEntityName = () => 'Glossary Term';

    renderProfile = (urn) => {
        return (
            <EntityProfile
                urn={urn}
                entityType={EntityType.GlossaryTerm}
                useEntityQuery={useGetGlossaryTermQuery as any}
                showDeprecateOption
                tabs={[
                    {
                        name: 'Related Entities',
                        component: GlossaryRelatedEntity,
                    },
                    {
                        name: 'Documentation',
                        component: DocumentationTab,
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
                sidebarSections={[
                    {
                        component: SidebarAboutSection,
                    },
                    {
                        component: SidebarOwnerSection,
                    },
                ]}
                getOverrideProperties={this.getOverridePropertiesFromEntity}
            />
        );
    };

    getOverridePropertiesFromEntity = (glossaryTerm?: GlossaryTerm | null): GenericEntityProperties => {
        // if dataset has subTypes filled out, pick the most specific subtype and return it
        return {
            customProperties: glossaryTerm?.properties?.customProperties,
        };
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as GlossaryTerm);
    };

    renderPreview = (_: PreviewType, data: GlossaryTerm) => {
        return (
            <Preview
                urn={data?.urn}
                name={this.displayName(data)}
                description={data?.properties?.description || ''}
                owners={data?.ownership?.owners}
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
}
