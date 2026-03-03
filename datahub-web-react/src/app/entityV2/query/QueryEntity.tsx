import { FileOutlined } from '@ant-design/icons';
import { FileSql } from '@phosphor-icons/react';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, IconStyleType } from '@app/entityV2/Entity';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import SidebarQueryDefinitionSection from '@app/entityV2/shared/containers/profile/sidebar/Query/SidebarQueryDefinitionSection';
import SidebarQueryDescriptionSection from '@app/entityV2/shared/containers/profile/sidebar/Query/SidebarQueryDescriptionSection';
import SidebarQueryOperationsSection from '@app/entityV2/shared/containers/profile/sidebar/Query/SidebarQueryOperationsSection';
import SidebarQueryUpdatedAtSection from '@app/entityV2/shared/containers/profile/sidebar/Query/SidebarQueryUpdatedAtSection';
import { SidebarQueryLogicSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarLogicSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';

import { useGetQueryQuery } from '@graphql/query.generated';
import { DataPlatform, EntityType, QueryEntity as Query } from '@types';

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

    getEntityName = () => 'Query';

    getCollectionName = () => 'Queries';

    useEntityQuery = useGetQueryQuery;

    renderProfile = (urn: string) => {
        return (
            <EntityProfile
                urn={urn}
                entityType={EntityType.Query}
                useEntityQuery={useGetQueryQuery}
                tabs={[
                    {
                        name: 'Documentation',
                        component: DocumentationTab,
                        icon: FileOutlined,
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
        return data?.properties?.name || (data?.properties?.source === 'SYSTEM' && 'System Query') || data?.urn;
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
