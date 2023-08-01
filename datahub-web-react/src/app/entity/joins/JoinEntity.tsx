import * as React from 'react';
import { DatabaseOutlined, DatabaseFilled } from '@ant-design/icons';
import { EntityType, Join, OwnershipType, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { GenericEntityProperties } from '../shared/types';
import { JoinPreviewCard } from './preview/JoinPreviewCard';
import joinIcon from '../../../images/joinIcon.svg';
import { JoinTab } from '../shared/tabs/Join/JoinTab';
import { useGetJoinQuery, useUpdateJoinMutation } from '../../../graphql/join.generated';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import './preview/JoinAction.less';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';

/**
 * Definition of the DataHub Join entity.
 */

export class JoinEntity implements Entity<Join> {
    type: EntityType = EntityType.Join;

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

        return <img src={joinIcon} style={{ height: '16px', width: '16px' }} alt="" />;
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'join';

    getCollectionName = () => '';

    getEntityName = () => 'Join';

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Join}
            useEntityQuery={useGetJoinQuery}
            useUpdateQuery={useUpdateJoinMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            tabs={[
                {
                    name: 'Join',
                    component: JoinTab,
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

    getOverridePropertiesFromEntity = (_join?: Join | null): GenericEntityProperties => {
        return {};
    };

    renderPreview = (_: PreviewType, data: Join) => {
        return (
            <>
                <JoinPreviewCard
                    urn={data.urn}
                    name={
                        <span className="joinName">{data.properties?.name || data.editableProperties?.name || ''}</span>
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
        return this.renderPreview(PreviewType.SEARCH, result.entity as Join);
    };

    displayName = (data: Join) => {
        return data.properties?.name || data.editableProperties?.name || data.urn;
    };

    getGenericEntityProperties = (data: Join) => {
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
