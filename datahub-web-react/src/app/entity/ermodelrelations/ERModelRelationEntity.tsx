import * as React from 'react';
import { DatabaseOutlined, DatabaseFilled } from '@ant-design/icons';
import { EntityType, ErModelRelation, OwnershipType, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { GenericEntityProperties } from '../shared/types';
import { ERModelRelationPreviewCard } from './preview/ERModelRelationPreviewCard';
import ermodelrelationIcon from '../../../images/ermodelrelationIcon.svg';
import { ERModelRelationTab } from '../shared/tabs/ERModelRelation/ERModelRelationTab';
import { useGetErModelRelationQuery, useUpdateErModelRelationshipMutation } from '../../../graphql/ermodelrelation.generated';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import './preview/ERModelRelationAction.less';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';

/**
 * Definition of the DataHub ERModelRelation entity.
 */

export class ERModelRelationEntity implements Entity<ErModelRelation> {
    type: EntityType = EntityType.Ermodelrelation;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DatabaseOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DatabaseFilled style={{ fontSize, color: '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
        }

        return <img src={ermodelrelationIcon} style={{ height: '16px', width: '16px' }} alt="" />;
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'ermodelrelation';

    getCollectionName = () => '';

    getEntityName = () => 'ER-Model-Relation';

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Ermodelrelation}
            useEntityQuery={useGetErModelRelationQuery}
            useUpdateQuery={useUpdateErModelRelationshipMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            tabs={[
                {
                    name: 'ER-Model-Relation',
                    component: ERModelRelationTab,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
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
                    properties: {
                        defaultOwnerType: OwnershipType.TechnicalOwner,
                    },
                },
                {
                    component: SidebarTagsSection,
                    properties: {
                        hasTags: true,
                        hasTerms: true,
                    },
                },
            ]}
            isNameEditable
        />
    );

    getOverridePropertiesFromEntity = (_ermodelrelation?: ErModelRelation | null): GenericEntityProperties => {
        return {};
    };

    renderPreview = (_: PreviewType, data: ErModelRelation) => {
        return (
            <>
                <ERModelRelationPreviewCard
                    urn={data.urn}
                    name={
                        <span className="ermodelrelationName">{data.properties?.name || data.editableProperties?.name || ''}</span>
                    }
                    description={data?.editableProperties?.description || ''}
                    owners={data.ownership?.owners}
                    glossaryTerms={data?.glossaryTerms || undefined}
                    globalTags={data?.tags}
                />
            </>
        );
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as ErModelRelation);
    };

    displayName = (data: ErModelRelation) => {
        return data.properties?.name || data.editableProperties?.name || data.urn;
    };

    getGenericEntityProperties = (data: ErModelRelation) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    supportedCapabilities = () => {
        return new Set([]);
    };
}
