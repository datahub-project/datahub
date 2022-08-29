import * as React from 'react';
import { FolderOutlined } from '@ant-design/icons';
import { Domain, EntityType, SearchResult } from '../../../types.generated';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { useGetDomainQuery } from '../../../graphql/domain.generated';
import { DomainEntitiesTab } from './DomainEntitiesTab';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityDropdown';
import { EntityActionItem } from '../shared/entity/EntityActions';
// import { EntityActionItem } from '../shared/entity/EntityActions';

/**
 * Definition of the DataHub Domain entity.
 */
export class DomainEntity implements Entity<Domain> {
    type: EntityType = EntityType.Domain;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <FolderOutlined />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <FolderOutlined style={{ fontSize, color: '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
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

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'domain';

    getEntityName = () => 'Domain';

    getCollectionName = () => 'Domains';

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Domain}
            useEntityQuery={useGetDomainQuery}
            useUpdateQuery={undefined}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={new Set([EntityMenuItems.COPY_URL, EntityMenuItems.DELETE])}
            headerActionItems={new Set([EntityActionItem.BATCH_ADD_DOMAIN])}
            isNameEditable
            tabs={[
                {
                    name: 'Entities',
                    component: DomainEntitiesTab,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
            ]}
            sidebarSections={[
                {
                    component: SidebarAboutSection,
                },
                {
                    component: SidebarOwnerSection,
                    properties: {
                        hideOwnerType: true,
                    },
                },
            ]}
        />
    );

    renderPreview = (_: PreviewType, data: Domain) => {
        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                count={data.entities?.total}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Domain;
        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                count={data.entities?.total}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
            />
        );
    };

    displayName = (data: Domain) => {
        return data?.properties?.name || data?.id || data.urn;
    };

    getOverridePropertiesFromEntity = (data: Domain) => {
        return {
            name: data.properties?.name,
        };
    };

    getGenericEntityProperties = (data: Domain) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    supportedCapabilities = () => {
        // TODO.. Determine whether SOFT_DELETE should go into here.
        return new Set([EntityCapabilityType.OWNERS]);
    };
}
