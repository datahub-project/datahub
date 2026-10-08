import { File } from '@phosphor-icons/react/dist/csr/File';
import { FileSql } from '@phosphor-icons/react/dist/csr/FileSql';
import i18next from 'i18next';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, IconStyleType } from '@app/entityV2/Entity';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/entityData';
import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';

import { useGetQueryQuery } from '@graphql/query.generated';
import { DataPlatform, EntityType, QueryEntity as Query } from '@types';

const EntityProfile = lazyProfileComponent('EntityProfile', () =>
    import('@app/entityV2/shared/containers/profile/EntityProfile').then((module) => ({
        default: module.EntityProfile,
    })),
);
const SidebarQueryDefinitionSection = lazyProfileComponent(
    'SidebarQueryDefinitionSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/Query/SidebarQueryDefinitionSection'),
);
const SidebarQueryDescriptionSection = lazyProfileComponent(
    'SidebarQueryDescriptionSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/Query/SidebarQueryDescriptionSection'),
);
const SidebarQueryOperationsSection = lazyProfileComponent(
    'SidebarQueryOperationsSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/Query/SidebarQueryOperationsSection'),
);
const SidebarQueryUpdatedAtSection = lazyProfileComponent(
    'SidebarQueryUpdatedAtSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/Query/SidebarQueryUpdatedAtSection'),
);
const SidebarQueryLogicSection = lazyProfileComponent('SidebarQueryLogicSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/SidebarLogicSection').then((module) => ({
        default: module.SidebarQueryLogicSection,
    })),
);
const DocumentationTab = lazyProfileComponent('DocumentationTab', () =>
    import('@app/entityV2/shared/tabs/Documentation/DocumentationTab').then((module) => ({
        default: module.DocumentationTab,
    })),
);

/**
 * Definition of the DataHub DataPlatformInstance entity.
 * Most of this still needs to be filled out.
 */
export class QueryEntity implements Entity<Query> {
    type: EntityType = EntityType.Query;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <FileSql
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
            />
        );
    };

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'query';

    getEntityName = () => i18next.t('entity.types:query.name');

    getCollectionName = () => i18next.t('entity.types:query.namePlural');

    useEntityQuery = useGetQueryQuery;

    renderProfile = (urn: string) => {
        return (
            <EntityProfile
                urn={urn}
                entityType={EntityType.Query}
                useEntityQuery={useGetQueryQuery}
                tabs={[
                    {
                        name: i18next.t('entity.types:tab.documentation'),
                        component: DocumentationTab,
                        icon: File,
                    },
                ]}
                sidebarSections={[
                    { component: SidebarQueryUpdatedAtSection },
                    { component: SidebarQueryDefinitionSection },
                    { component: SidebarQueryLogicSection },
                    { component: SidebarQueryDescriptionSection },
                    { component: SidebarQueryOperationsSection },
                ]}
                sidebarTabs={[]}
                getOverrideProperties={() => ({})}
            />
        );
    };

    getOverridePropertiesFromEntity = (query?: Query | null): GenericEntityProperties => {
        return {
            name: query && this.displayName(query),
            platform: query?.platform,
        };
    };

    renderEmbeddedProfile = (_: string) => <></>;

    renderPreview = () => {
        return <></>;
    };

    renderSearch = () => {
        return <></>;
    };

    getLineageVizConfig = (query: Query) => {
        // TODO: Set up types better here
        const platform: DataPlatform | undefined = (query as any)?.queryPlatform;
        return {
            urn: query.urn,
            name: query.properties?.name || query.urn,
            type: EntityType.Query,
            icon: platform?.properties?.logoUrl || undefined,
            platform: platform || undefined,
        };
    };

    displayName = (data: Query) => {
        return (
            data?.properties?.name ||
            (data?.properties?.source === 'SYSTEM' && i18next.t('entity.types:query.systemQueryFallback')) ||
            data?.urn
        );
    };

    getGenericEntityProperties = (data: Query) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    supportedCapabilities = () => {
        return new Set([]);
    };

    getGraphName = () => {
        return 'query';
    };
}
