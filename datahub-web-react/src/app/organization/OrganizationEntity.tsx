import { BankOutlined } from '@ant-design/icons';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entity/Entity';
import { EntityMenuItems } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { EntityProfile } from '@app/entity/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entity/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarOwnerSection } from '@app/entity/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { getDataForEntityType } from '@app/entity/shared/containers/profile/utils';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { PropertiesTab } from '@app/entity/shared/tabs/Properties/PropertiesTab';
import { OrganizationDocumentationTab } from '@app/organization/OrganizationDocumentationTab';
import { Preview } from '@app/organization/preview/Preview';
import { EntityType, Organization, SearchResult } from '@src/types.generated';

import { useGetOrganizationQuery } from '@graphql/organization.generated';

export class OrganizationEntity implements Entity<Organization> {
    type: EntityType = EntityType.Organization;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        return (
            <BankOutlined
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'organization';

    getPathName = () => 'organization';

    getEntityName = () => 'Organization';

    getCollectionName = () => 'Organizations';

    useEntityQuery = useGetOrganizationQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Organization}
            useEntityQuery={useGetOrganizationQuery}
            isNameEditable
            headerDropdownItems={new Set([EntityMenuItems.DELETE])}
            tabs={[
                {
                    name: 'Documentation',
                    component: OrganizationDocumentationTab,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
            ]}
            sidebarSections={this.getSidebarSections()}
            getOverrideProperties={this.getOverrideProperties}
        />
    );

    getSidebarSections = () => [
        {
            component: SidebarAboutSection,
        },
        {
            component: SidebarOwnerSection,
        },
    ];

    renderPreview = (_: PreviewType, data: Organization) => {
        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Organization;
        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
            />
        );
    };

    displayName = (data: Organization) => {
        return data?.properties?.name || data?.urn;
    };

    getGenericEntityProperties = (data: Organization) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: (orgData) => orgData,
        });
    };

    getOverrideProperties = (data: Organization): GenericEntityProperties => {
        return {
            name: data?.properties?.name,
            customProperties: data?.properties?.customProperties,
        } as GenericEntityProperties;
    };

    supportedCapabilities = () => {
        return new Set([EntityCapabilityType.OWNERS]);
    };
}
