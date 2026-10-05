import { ApiOutlined, FileOutlined, PartitionOutlined, ReadOutlined, UnorderedListOutlined } from '@ant-design/icons';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { Wrench } from '@phosphor-icons/react/dist/csr/Wrench';
import i18next from 'i18next';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { ApiSummaryTab } from '@app/entityV2/api/ApiSummaryTab';
import SignatureTab from '@app/entityV2/api/SignatureTab';
import { Preview } from '@app/entityV2/api/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfileTab } from '@app/entityV2/shared/constants';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import SidebarStructuredProperties from '@app/entityV2/shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { getFirstSubType } from '@app/entityV2/shared/utils';

import { useGetApiQuery } from '@graphql/api.generated';
import { Api, EntityType, SearchResult } from '@types';

const headerDropdownItems = new Set([
    EntityMenuItems.SHARE,
    EntityMenuItems.LINK_VERSION,
    EntityMenuItems.CHANGE_HISTORY,
    EntityMenuItems.DELETE,
]);

/**
 * Definition of the DataHub API entity. An API is a named callable with a
 * typed input and output schema (an MCP tool, REST endpoint, gRPC method,
 * function, etc.) that can be invoked by humans, services, or AI agents.
 */
export class ApiEntity implements Entity<Api> {
    type: EntityType = EntityType.Api;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <Wrench
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    // NOT 'api' — the frontend server reserves /api/* as the GMS proxy route,
    // so an 'api' path segment is intercepted before the SPA router sees it.
    getPathName = () => 'apis';

    getEntityName = () => i18next.t('entity.types:api.name');

    getCollectionName = () => i18next.t('entity.types:api.namePlural');

    useEntityQuery = useGetApiQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Api}
            useEntityQuery={useGetApiQuery}
            useUpdateQuery={undefined}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            isNameEditable={false}
            tabs={[
                {
                    id: EntityProfileTab.SUMMARY_TAB,
                    name: i18next.t('entity.types:tab.summary'),
                    component: ApiSummaryTab,
                    icon: ReadOutlined,
                },
                {
                    name: i18next.t('entity.types:tab.documentation'),
                    component: DocumentationTab,
                    icon: FileOutlined,
                },
                {
                    name: i18next.t('entity.types:tab.signature'),
                    component: SignatureTab,
                    icon: ApiOutlined,
                },
                {
                    name: i18next.t('entity.types:tab.lineage'),
                    component: LineageTab,
                    icon: PartitionOutlined,
                },
                {
                    name: i18next.t('entity.types:tab.properties'),
                    component: PropertiesTab,
                    icon: UnorderedListOutlined,
                },
            ]}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    getSidebarSections = () => [
        {
            component: SidebarEntityHeader,
        },
        {
            component: SidebarAboutSection,
        },
        {
            component: SidebarOwnerSection,
        },
        {
            component: SidebarDomainSection,
            properties: {
                updateOnly: true,
            },
        },
        {
            component: DataProductSection,
        },
        {
            component: SidebarTagsSection,
        },
        {
            component: SidebarGlossaryTermsSection,
        },
        {
            component: SidebarStructuredProperties,
        },
        {
            component: StatusSection,
        },
    ];

    getSidebarTabs = () => [
        {
            name: i18next.t('entity.types:tab.properties'),
            component: PropertiesTab,
            description: i18next.t('entity.types:sidebar.propertiesDescription'),
            icon: ListBullets,
        },
    ];

    renderPreview = (previewType: PreviewType, data: Api, actions) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.properties?.name || ''}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                globalTags={data.tags}
                glossaryTerms={data.glossaryTerms}
                domain={data.domain?.domain}
                externalUrl={data.properties?.externalUrl}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
                actions={actions}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Api;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.properties?.name || ''}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                globalTags={data.tags}
                glossaryTerms={data.glossaryTerms}
                domain={data.domain?.domain}
                externalUrl={data.properties?.externalUrl}
                degree={(result as any).degree}
                paths={(result as any).paths}
                headerDropdownItems={headerDropdownItems}
                previewType={PreviewType.SEARCH}
            />
        );
    };

    displayName = (data: Api) => {
        return data?.properties?.name || data.urn;
    };

    // Required for the entity to hydrate as a lineage-graph node — without it
    // getLineageVizConfigV2 returns null and the node renders a perpetual skeleton.
    getLineageVizConfig = (entity: Api) => {
        return {
            urn: entity.urn,
            name: this.displayName(entity),
            type: EntityType.Api,
            subtype: getFirstSubType(entity),
            icon: undefined,
            platform: entity?.dataPlatformInstance?.platform ?? undefined,
        };
    };

    getOverridePropertiesFromEntity = (data: Api) => {
        const name = data?.properties?.name;
        const externalUrl = data?.properties?.externalUrl;
        return {
            name,
            externalUrl,
            platform: data?.dataPlatformInstance?.platform,
        };
    };

    getGenericEntityProperties = (data: Api) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    supportedCapabilities = () => {
        return new Set([
            EntityCapabilityType.OWNERS,
            EntityCapabilityType.GLOSSARY_TERMS,
            EntityCapabilityType.TAGS,
            EntityCapabilityType.DOMAINS,
            EntityCapabilityType.DATA_PRODUCTS,
            EntityCapabilityType.LINEAGE,
        ]);
    };

    getGraphName = () => {
        return 'api';
    };
}
