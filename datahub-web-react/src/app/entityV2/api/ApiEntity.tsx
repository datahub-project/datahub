import { BookOpen } from '@phosphor-icons/react/dist/csr/BookOpen';
import { File } from '@phosphor-icons/react/dist/csr/File';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { Plugs } from '@phosphor-icons/react/dist/csr/Plugs';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import { Wrench } from '@phosphor-icons/react/dist/csr/Wrench';
import i18next from 'i18next';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/api/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfileTab } from '@app/entityV2/shared/constants';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/entityData';
import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import {
    DataProductSection,
    DocumentationTab,
    EntityProfile,
    LineageTab,
    PropertiesTab,
    SidebarAboutSection,
    SidebarDomainSection,
    SidebarEntityHeader,
    SidebarGlossaryTermsSection,
    SidebarOwnerSection,
    SidebarStructuredProperties,
    SidebarTagsSection,
    StatusSection,
} from '@app/entityV2/shared/profileChunks';
import { getFirstSubType } from '@app/entityV2/shared/utils';

import { useGetApiQuery } from '@graphql/api.generated';
import { Api, EntityType, SearchResult } from '@types';

const ApiSummaryTab = lazyProfileComponent('ApiSummaryTab', () =>
    import('@app/entityV2/api/ApiSummaryTab').then((module) => ({
        default: module.ApiSummaryTab,
    })),
);
const SignatureTab = lazyProfileComponent('SignatureTab', () => import('@app/entityV2/api/SignatureTab'));

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
                    icon: BookOpen,
                },
                {
                    name: i18next.t('entity.types:tab.documentation'),
                    component: DocumentationTab,
                    icon: File,
                },
                {
                    name: i18next.t('entity.types:tab.signature'),
                    component: SignatureTab,
                    icon: Plugs,
                },
                {
                    name: i18next.t('entity.types:tab.lineage'),
                    component: LineageTab,
                    icon: TreeStructure,
                },
                {
                    name: i18next.t('entity.types:tab.properties'),
                    component: PropertiesTab,
                    icon: ListBullets,
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
