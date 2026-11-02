import { FileOutlined, PartitionOutlined, ReadOutlined, UnorderedListOutlined } from '@ant-design/icons';
import { GitBranch } from '@phosphor-icons/react/dist/csr/GitBranch';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import i18next from 'i18next';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/repository/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfileTab, EntityTabPath } from '@app/entityV2/shared/constants';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/entityData';
import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import {
    DocumentationTab,
    EntityProfile,
    LineageTab,
    PropertiesTab,
    SidebarAboutSection,
    SidebarDomainSection,
    SidebarEntityHeader,
    SidebarGlossaryTermsSection,
    SidebarLineageSection,
    SidebarOwnerSection,
    SidebarStructuredProperties,
    SidebarTagsSection,
    StatusSection,
} from '@app/entityV2/shared/profileChunks';
import { useAppConfig } from '@app/useAppConfig';

import { GetRepositoryQuery, useGetRepositoryQuery } from '@graphql/repository.generated';
import { EntityType, Repository, SearchResult } from '@types';

const RepositorySummaryTab = lazyProfileComponent('RepositorySummaryTab', () =>
    import('@app/entityV2/repository/RepositorySummaryTab').then((module) => ({
        default: module.RepositorySummaryTab,
    })),
);

const headerDropdownItems = new Set([EntityMenuItems.SHARE, EntityMenuItems.CHANGE_HISTORY]);

/**
 * Definition of the DataHub Repository entity. A Repository is a source-code
 * repository (GitHub, GitLab, internal SCM, etc.) — the genesis node of the
 * software-to-data lifecycle (repo -> service -> api -> app -> dataset). It is
 * not tabular: no columns, queries, stats, quality, or preview.
 */
export class RepositoryEntity implements Entity<Repository> {
    type: EntityType = EntityType.Repository;

    appconfig = useAppConfig;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <GitBranch
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

    getPathName = () => 'repository';

    getEntityName = () => i18next.t('entity.types:repository.name');

    getCollectionName = () => i18next.t('entity.types:repository.namePlural');

    useEntityQuery = useGetRepositoryQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Repository}
            useEntityQuery={useGetRepositoryQuery}
            useUpdateQuery={undefined}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            isNameEditable={false}
            tabs={[
                {
                    id: EntityProfileTab.SUMMARY_TAB,
                    name: i18next.t('entity.types:tab.summary'),
                    path: EntityTabPath.SUMMARY,
                    component: RepositorySummaryTab,
                    icon: ReadOutlined,
                },
                {
                    name: i18next.t('entity.types:tab.documentation'),
                    path: EntityTabPath.DOCUMENTATION,
                    component: DocumentationTab,
                    icon: FileOutlined,
                },
                {
                    name: i18next.t('entity.types:tab.lineage'),
                    path: EntityTabPath.LINEAGE,
                    component: LineageTab,
                    icon: PartitionOutlined,
                },
                {
                    name: i18next.t('entity.types:tab.properties'),
                    path: EntityTabPath.PROPERTIES,
                    component: PropertiesTab,
                    icon: UnorderedListOutlined,
                    getCount: (_, repository: GetRepositoryQuery) => {
                        return repository?.repository?.structuredProperties?.properties?.length || 0;
                    },
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
            component: SidebarLineageSection,
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

    renderPreview = (previewType: PreviewType, data: Repository, actions) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                description={data.properties?.description}
                // repositoryPlatform is a query-side alias for the schema's `platform` field
                // (see search.graphql/preview.graphql), used by the shared entityPreview
                // fragment that hover cards and entity-reference chips render through. The
                // profile page's own getRepository query does not alias it, so read either
                // shape depending on which query populated `data`.
                platform={data.platform || (data as any).repositoryPlatform}
                owners={data.ownership?.owners}
                globalTags={data.tags}
                glossaryTerms={data.glossaryTerms}
                domain={data.domain?.domain}
                externalUrl={data.source?.externalUrl}
                browsePaths={data.browsePathV2 || undefined}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
                actions={actions}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        // Search results alias Repository.platform as `repositoryPlatform` (see
        // search.graphql / preview.graphql): Dataset.platform is non-null and shares
        // the polymorphic selection set, so an unaliased Repository.platform would be
        // a GraphQL fields-conflict error.
        const data = result.entity as Repository & { repositoryPlatform?: Repository['platform'] };
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                description={data.properties?.description}
                platform={data.repositoryPlatform}
                owners={data.ownership?.owners}
                globalTags={data.tags}
                glossaryTerms={data.glossaryTerms}
                domain={data.domain?.domain}
                externalUrl={data.source?.externalUrl}
                degree={(result as any).degree}
                paths={(result as any).paths}
                browsePaths={data.browsePathV2 || undefined}
                headerDropdownItems={headerDropdownItems}
                previewType={PreviewType.SEARCH}
            />
        );
    };

    getLineageVizConfig = (entity: Repository) => {
        return {
            urn: entity.urn,
            name: this.displayName(entity),
            type: EntityType.Repository,
            icon: undefined,
            // The lineage fragment aliases platform as repositoryPlatform (see lineage.graphql).
            platform:
                entity?.platform ??
                (entity as Repository & { repositoryPlatform?: Repository['platform'] })?.repositoryPlatform ??
                undefined,
        };
    };

    displayName = (data: Repository) => {
        return data?.properties?.name || data.urn;
    };

    getOverridePropertiesFromEntity = (data: Repository) => {
        // Called from both the profile-page path (plain `platform` field) and the
        // search/autocomplete path (aliased `repositoryPlatform`, to dodge a GraphQL
        // SameResponseShape conflict with Dataset's non-null platform field in the
        // shared search.graphql fragments) — read either shape.
        return {
            name: data?.properties?.name,
            platform: data?.platform || (data as any).repositoryPlatform,
            externalUrl: data?.source?.externalUrl,
        };
    };

    getGenericEntityProperties = (data: Repository) => {
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
            EntityCapabilityType.LINEAGE,
        ]);
    };

    getGraphName = () => {
        return 'repository';
    };
}
