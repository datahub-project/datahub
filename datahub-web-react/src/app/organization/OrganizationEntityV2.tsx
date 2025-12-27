import { AppstoreOutlined, BankOutlined, TeamOutlined } from '@ant-design/icons';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { useGetOrganizationQuery } from '../../graphql/organization.generated';
import { Organization, EntityType, SearchResult } from '../../types.generated';
import { OrganizationEntitiesTab } from './OrganizationEntitiesTab';
import { OrganizationMembers } from './OrganizationMembers';
import { PreviewV2 } from './preview/PreviewV2';
import { OrganizationDocumentationTab } from './OrganizationDocumentationTab';

export class OrganizationEntityV2 implements Entity<Organization> {
    type: EntityType = EntityType.Organization;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
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
            tabs={[
                {
                    name: 'Entities',
                    component: OrganizationEntitiesTab,
                    icon: AppstoreOutlined,
                },
                {
                    name: 'Members',
                    component: OrganizationMembers,
                    icon: TeamOutlined,
                },
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

    renderPreview = (previewType: PreviewType, data: Organization) => {
        return (
            <PreviewV2
                urn={data.urn}
                name={this.displayName(data)}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Organization;
        return (
            <PreviewV2
                urn={data.urn}
                name={this.displayName(data)}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
                previewType={PreviewType.SEARCH}
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
            getOverrideProperties: (data) => data,
        });
    };

    supportedCapabilities = () => {
        return new Set([EntityCapabilityType.OWNERS]);
    };
}
