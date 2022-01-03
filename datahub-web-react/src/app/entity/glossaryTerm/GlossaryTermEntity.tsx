import * as React from 'react';
import { BookFilled, BookOutlined } from '@ant-design/icons';
import { EntityType, GlossaryTerm, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { useGetGlossaryTermQuery, useUpdateGlossaryTermMutation } from '../../../graphql/glossaryTerm.generated';
import { GenericEntityProperties } from '../shared/types';
import { SchemaTab } from '../shared/tabs/Dataset/Schema/SchemaTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import GlossaryRelatedEntity from './profile/GlossaryRelatedEntity';
import GlossayRelatedTerms from './profile/GlossaryRelatedTerms';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';

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
                useUpdateQuery={useUpdateGlossaryTermMutation as any}
                tabs={[
                    {
                        name: 'Schema',
                        component: SchemaTab,
                    },
                    {
                        name: 'Related Entities',
                        component: GlossaryRelatedEntity,
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
                properties={{
                    hideProfileNavBar: true,
                }}
            />
        );
    };

    getOverridePropertiesFromEntity = (): GenericEntityProperties => {
        // if dataset has subTypes filled out, pick the most specific subtype and return it
        return {};
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as GlossaryTerm);
    };

    renderPreview = (_: PreviewType, data: GlossaryTerm) => {
        return (
            <Preview
                urn={data?.urn}
                name={data?.name}
                definition={data?.glossaryTermInfo?.definition}
                owners={data?.ownership?.owners}
            />
        );
    };

    displayName = (data: GlossaryTerm) => {
        return data.name;
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
